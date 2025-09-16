import os
import time
import json
import sqlite3
import requests
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from flask import Flask, jsonify
from google.cloud import firestore  
from sendgrid import SendGridAPIClient  
from sendgrid.helpers.mail import Mail
from typing import Dict, Optional, List

class Config:
    DEPTH_STD_THRESHOLD = 1.0  # 市場深度標準差
    VRP_MIN_THRESHOLD = 0.01   # VRP最小值 1%
    SPREAD_MAX_BPS = 20        # 價差最大值 20 bps
    RSI_OVERBOUGHT = 80        # RSI超買
    RSI_OVERSOLD = 20          # RSI超賣
    EMAIL_COOLDOWN = 1800      # 30分鐘
    
    SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
    SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'alerts@sol-monitor.com')
    RECIPIENTS = os.environ.get('RECIPIENTS', '').split(',')
    
    COLLECTION_MARKET = 'market_data'
    COLLECTION_ALERTS = 'alerts'
    COLLECTION_STATS = 'statistics'

class DataCollector:    
    def __init__(self, logger):
        self.logger = logger
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.session.headers.update({
            'User-Agent': 'SOL-Monitor/1.0',
            'Accept': 'application/json'
        })
        
    def fetch_orderbook_any_source(self) -> Optional[Dict]:
        import concurrent.futures
        
        sources = [
            {
                'name': 'KuCoin',
                'url': 'https://api.kucoin.com/api/v1/market/orderbook/level2_100',
                'params': {'symbol': 'SOL-USDT'}
            },
            {
                'name': 'Gate.io',
                'url': 'https://api.gateio.ws/api/v4/spot/order_book',
                'params': {'currency_pair': 'SOL_USDT', 'limit': 100}
            },
            {
                'name': 'MEXC',
                'url': 'https://api.mexc.com/api/v3/depth',
                'params': {'symbol': 'SOLUSDT', 'limit': 100}
            }
        ]
        
        def fetch_source(source):
            try:
                response = self.session.get(
                    source['url'],
                    params=source['params'],
                    timeout=5  
                )
                if response.status_code == 200:
                    return {'name': source['name'], 'data': response.json()}
            except:
                pass
            return None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fetch_source, source) for source in sources]
            for future in concurrent.futures.as_completed(futures, timeout=6):
                result = future.result()
                if result:
                    data = self._parse_orderbook(result['name'], result['data'])
                    if data:
                        self.logger.info(f"✅ Using {result['name']}")
                        return {'source': result['name'], 'data': data}
        
        self.logger.error("❌ All sources failed")
        return None
    
    def _parse_orderbook(self, source: str, data: Dict) -> Optional[Dict]:
        try:
            if source == 'KuCoin' and data.get('code') == '200000':
                return data.get('data')
            elif source in ['Gate.io', 'MEXC'] and 'bids' in data:
                return data
        except:
            pass
        return None
    
    def fetch_price_data(self) -> Optional[Dict]:
        try:
            response = self.session.get(
                'https://api.coingecko.com/api/v3/simple/price',
                params={
                    'ids': 'solana',
                    'vs_currencies': 'usd',
                    'include_24hr_vol': 'true',
                    'include_24hr_change': 'true',
                    'include_market_cap': 'true'
                },
                timeout=5
            )
            if response.status_code == 200:
                data = response.json().get('solana', {})
                return {
                    'price': data.get('usd'),
                    'volume_24h': data.get('usd_24h_vol', 0),
                    'change_24h': data.get('usd_24h_change', 0),
                    'market_cap': data.get('usd_market_cap', 0)
                }
        except Exception as e:
            self.logger.error(f"Price fetch error: {e}")
        return None

# ==================== 技術指標計算器 ====================
class TechnicalIndicators:
    
    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
        if len(prices) < period + 1:
            return None
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def calculate_vrp(prices: List[float], change_24h: float) -> Optional[float]:
        """計算簡化的VRP"""
        if len(prices) < 24:
            return None
        # RV
        returns = np.diff(np.log(prices))
        rv = np.std(returns) * np.sqrt(365 * 24 * 12)  # 年化
        # IV
        iv_proxy = abs(change_24h) / 100 * np.sqrt(365)
        vrp = iv_proxy - rv
        return vrp

class SOLMonitor:
    def __init__(self):
        self.setup_logging()
        self.collector = DataCollector(self.logger)
        self.indicators = TechnicalIndicators()
        self.db = firestore.Client()
        self.sg = SendGridAPIClient(Config.SENDGRID_API_KEY) if Config.SENDGRID_API_KEY else None
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def calculate_market_metrics(self, orderbook_data: Dict) -> Optional[Dict]:
        try:
            data = orderbook_data['data']
            source = orderbook_data['source']
            bids = [(float(b[0]), float(b[1])) for b in data.get('bids', [])[:50]]
            asks = [(float(a[0]), float(a[1])) for a in data.get('asks', [])[:50]]
            
            if not bids or not asks:
                return None
                        best_bid, best_ask = bids[0][0], asks[0][0]
            mid_price = (best_bid + best_ask) / 2
            bid_threshold = mid_price * 0.99
            ask_threshold = mid_price * 1.01
            
            bid_depth = sum(p * s for p, s in bids if p >= bid_threshold)
            ask_depth = sum(p * s for p, s in asks if p <= ask_threshold)
            
            total_depth = bid_depth + ask_depth
            depth_imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0
            spread_bps = ((best_ask - best_bid) / mid_price) * 10000
            
            return {
                'timestamp': datetime.utcnow(),
                'price': mid_price,
                'bid_depth': bid_depth,
                'ask_depth': ask_depth,
                'total_depth': total_depth,
                'depth_imbalance': depth_imbalance,
                'spread_bps': spread_bps,
                'source': source,
                'best_bid': best_bid,
                'best_ask': best_ask
            }
            
        except Exception as e:
            self.logger.error(f"Metrics calculation error: {e}")
            return None
    
    def get_historical_data(self, hours: int = 24) -> pd.DataFrame:
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            
            docs = self.db.collection(Config.COLLECTION_MARKET)\
                .where('timestamp', '>=', cutoff)\
                .order_by('timestamp', direction=firestore.Query.DESCENDING)\
                .limit(500)\
                .stream()
            
            data = [doc.to_dict() for doc in docs]
            
            if data:
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df.sort_values('timestamp')
            
        except Exception as e:
            self.logger.error(f"Historical data error: {e}")
        
        return pd.DataFrame()
    
    def check_alerts(self, metrics: Dict, price_data: Dict) -> List[Dict]:
        alerts = []
        
        df = self.get_historical_data(hours=24*30)  
        
        if len(df) >= 10:
            # 1. 市場深度警報
            mean_depth = df['total_depth'].mean()
            std_depth = df['total_depth'].std()
            threshold = mean_depth - Config.DEPTH_STD_THRESHOLD * std_depth
            
            if metrics['total_depth'] < threshold:
                alerts.append({
                    'type': 'DEPTH_DECLINE',
                    'severity': 'HIGH',
                    'message': f'Market depth ${metrics["total_depth"]:,.0f} below threshold ${threshold:,.0f}',
                    'current': metrics['total_depth'],
                    'threshold': threshold,
                    'mean': mean_depth,
                    'std': std_depth
                })
            
            # 2. VRP警報
            prices = df['price'].tail(24).tolist()
            if price_data and len(prices) > 2:
                vrp = self.indicators.calculate_vrp(prices, price_data['change_24h'])
                if vrp and vrp < Config.VRP_MIN_THRESHOLD:
                    alerts.append({
                        'type': 'LOW_VRP',
                        'severity': 'HIGH',
                        'message': f'VRP {vrp*100:.2f}% below {Config.VRP_MIN_THRESHOLD*100}% threshold',
                        'current': vrp,
                        'threshold': Config.VRP_MIN_THRESHOLD
                    })
            
            # 3. RSI警報
            rsi = self.indicators.calculate_rsi(prices)
            if rsi:
                if rsi > Config.RSI_OVERBOUGHT:
                    alerts.append({
                        'type': 'RSI_OVERBOUGHT',
                        'severity': 'MEDIUM',
                        'message': f'RSI {rsi:.1f} indicates overbought',
                        'current': rsi,
                        'threshold': Config.RSI_OVERBOUGHT
                    })
                elif rsi < Config.RSI_OVERSOLD:
                    alerts.append({
                        'type': 'RSI_OVERSOLD',
                        'severity': 'MEDIUM',
                        'message': f'RSI {rsi:.1f} indicates oversold',
                        'current': rsi,
                        'threshold': Config.RSI_OVERSOLD
                    })
        
        # 4. 價差警報
        if metrics['spread_bps'] > Config.SPREAD_MAX_BPS:
            alerts.append({
                'type': 'WIDE_SPREAD',
                'severity': 'MEDIUM',
                'message': f'Spread {metrics["spread_bps"]:.2f} bps exceeds {Config.SPREAD_MAX_BPS} bps',
                'current': metrics['spread_bps'],
                'threshold': Config.SPREAD_MAX_BPS
            })
        
        # 5. 深度失衡警報
        if abs(metrics.get('depth_imbalance', 0)) > 0.3: 
            alerts.append({
                'type': 'DEPTH_IMBALANCE',
                'severity': 'LOW',
                'message': f'Order book imbalance: {metrics["depth_imbalance"]*100:.1f}%',
                'current': metrics['depth_imbalance'],
                'threshold': 0.3
            })
        
        return alerts
    
    def send_alert_email(self, alerts: List[Dict], metrics: Dict):
        """使用SendGrid發送Email"""
        if not self.sg or not alerts:
            return
        
        try:
            for alert in alerts:
                last_alert = self.db.collection(Config.COLLECTION_ALERTS)\
                    .where('type', '==', alert['type'])\
                    .where('email_sent', '==', True)\
                    .order_by('timestamp', direction=firestore.Query.DESCENDING)\
                    .limit(1).get()
                
                if last_alert:
                    last_time = last_alert[0].to_dict()['timestamp']
                    if (datetime.utcnow() - last_time).seconds < Config.EMAIL_COOLDOWN:
                        self.logger.info(f"Alert {alert['type']} in cooldown")
                        continue
                
                html_content = self._build_email_html(alert, metrics)
                
                message = Mail(
                    from_email=Config.SENDER_EMAIL,
                    to_emails=Config.RECIPIENTS,
                    subject=f"🚨 SOL Alert: {alert['type']}",
                    html_content=html_content
                )
                
                response = self.sg.send(message)
                
                if response.status_code in [200, 201, 202]:
                    self.db.collection(Config.COLLECTION_ALERTS).add({
                        'timestamp': datetime.utcnow(),
                        'type': alert['type'],
                        'severity': alert['severity'],
                        'message': alert['message'],
                        'current': alert['current'],
                        'threshold': alert['threshold'],
                        'email_sent': True
                    })
                    self.logger.info(f"✅ Alert sent: {alert['type']}")
                
        except Exception as e:
            self.logger.error(f"Email error: {e}")
    
    def _build_email_html(self, alert: Dict, metrics: Dict) -> str:
        """構建Email HTML"""
        return f"""
        <div style="font-family: Arial; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #e74c3c;">🚨 SOL Risk Alert</h2>
            
            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <h3>Alert: {alert['type']}</h3>
                <p><strong>Severity:</strong> {alert['severity']}</p>
                <p><strong>Message:</strong> {alert['message']}</p>
                <p><strong>Current Value:</strong> {alert['current']:.2f}</p>
                <p><strong>Threshold:</strong> {alert['threshold']:.2f}</p>
            </div>
            
            <div style="background: #d1ecf1; padding: 15px; border-radius: 8px;">
                <h3>Market Status</h3>
                <p><strong>Price:</strong> ${metrics['price']:.2f}</p>
                <p><strong>Depth:</strong> ${metrics['total_depth']:,.0f}</p>
                <p><strong>Spread:</strong> {metrics['spread_bps']:.2f} bps</p>
                <p><strong>Source:</strong> {metrics['source']}</p>
            </div>
            
            <div style="background: #fff3cd; padding: 15px; margin: 20px 0;">
                <h3>Recommendations</h3>
                <ul>
                    <li>Reduce leverage below 10x</li>
                    <li>Use limit orders only</li>
                    <li>Monitor liquidation levels</li>
                </ul>
            </div>
            
            <p style="color: #6c757d; font-size: 12px;">
                Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
                Next alert for this type: 30 minutes minimum
            </p>
        </div>
        """
    
    def run_cycle(self):
        try:
            start_time = time.time()
            self.logger.info("="*50)
            self.logger.info("Starting monitoring cycle...")
            
            orderbook = self.collector.fetch_orderbook_any_source()
            if not orderbook:
                self.logger.warning("No orderbook data")
                return
            
            metrics = self.calculate_market_metrics(orderbook)
            if not metrics:
                self.logger.warning("Failed to calculate metrics")
                return
            
            price_data = self.collector.fetch_price_data()
            if price_data:
                metrics.update(price_data)
            
            doc_ref = self.db.collection(Config.COLLECTION_MARKET).add(metrics)
            
            alerts = self.check_alerts(metrics, price_data)
            
            if alerts:
                self.send_alert_email(alerts, metrics)
            
            self._update_statistics(metrics, alerts)
            
            execution_time = time.time() - start_time
            self.logger.info(f"✅ Cycle complete in {execution_time:.2f}s")
            self.logger.info(f"   Price: ${metrics['price']:.2f}")
            self.logger.info(f"   Depth: ${metrics['total_depth']:,.0f}")
            self.logger.info(f"   Alerts: {len(alerts)}")
            
            return {
                'status': 'success',
                'metrics': metrics,
                'alerts': alerts,
                'execution_time': execution_time
            }
            
        except Exception as e:
            self.logger.error(f"Cycle error: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    def _update_statistics(self, metrics: Dict, alerts: List[Dict]):
        try:
            stats_ref = self.db.collection(Config.COLLECTION_STATS).document('latest')
            stats_ref.set({
                'last_update': datetime.utcnow(),
                'last_price': metrics['price'],
                'last_depth': metrics['total_depth'],
                'last_spread': metrics['spread_bps'],
                'total_alerts_today': len(alerts),
                'last_source': metrics['source']
            }, merge=True)
        except Exception as e:
            self.logger.error(f"Stats update error: {e}")

app = Flask(__name__)
monitor = SOLMonitor()

@app.route("/", methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'SOL Risk Monitor',
        'version': '2.0',
        'timestamp': datetime.utcnow().isoformat()
    })

@app.route("/run", methods=['POST', 'GET'])
def execute_task():
    try:
        result = monitor.run_cycle()
        return jsonify(result), 200 if result['status'] == 'success' else 500
    except Exception as e:
        logging.error(f"Execution error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route("/stats", methods=['GET'])
def get_stats():
    try:
        stats = monitor.db.collection(Config.COLLECTION_STATS).document('latest').get()
        if stats.exists:
            return jsonify(stats.to_dict())
        return jsonify({'message': 'No stats available'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route("/alerts", methods=['GET'])
def get_alerts():
    try:
        alerts = monitor.db.collection(Config.COLLECTION_ALERTS)\
            .order_by('timestamp', direction=firestore.Query.DESCENDING)\
            .limit(10).stream()
        
        result = []
        for doc in alerts:
            alert_data = doc.to_dict()
            alert_data['timestamp'] = alert_data['timestamp'].isoformat()
            result.append(alert_data)
        
        return jsonify({'alerts': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
