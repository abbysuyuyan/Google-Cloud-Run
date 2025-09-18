import os
import time
import json
import sqlite3
import requests
import logging
import smtplib
import pandas as pd
import numpy as np
from datetime import datetime
from flask import Flask, jsonify
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class Config:
    DEPTH_STD_THRESHOLD = 1.0     # Market Depth
    VRP_MIN_THRESHOLD = 0.01 
    RSI_OVERBOUGHT = 80 
    RSI_OVERSOLD = 20 
    SPREAD_MAX_BPS = 20 
    IMBALANCE_THRESHOLD = 0.3      # Order Book 30%
    EMAIL_COOLDOWN = 600          # 10分鐘冷卻
    DB_PATH = "/tmp/sol_risk.db"
    LOG_PATH = "/tmp/sol_risk.log"

class DataCollector:
    def __init__(self, logger):
        self.logger = logger
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        return session

    def fetch_with_retry(self, url, params=None, name="API", retries=3):
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                self.logger.warning(f"{name} returned status {response.status_code}")
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    self.logger.error(f"{name} failed: {e}")
        return None

    def fetch_orderbook_any_source(self):
        sources = [
            {'name': 'KuCoin', 'url': 'https://api.kucoin.com/api/v1/market/orderbook/level2_100', 'params': {'symbol': 'SOL-USDT'}, 'parser': lambda d: d.get('data') if d.get('code') == '200000' else None},
            {'name': 'Gate.io', 'url': 'https://api.gateio.ws/api/v4/spot/order_book', 'params': {'currency_pair': 'SOL_USDT', 'limit': 100}, 'parser': lambda d: d if 'bids' in d and 'asks' in d else None},
            {'name': 'MEXC', 'url': 'https://api.mexc.com/api/v3/depth', 'params': {'symbol': 'SOLUSDT', 'limit': 100}, 'parser': lambda d: d if 'bids' in d and 'asks' in d else None}
        ]
        for source in sources:
            self.logger.info(f"Trying {source['name']}...")
            data = self.fetch_with_retry(source['url'], source['params'], source['name'])
            if data:
                parsed = source['parser'](data)
                if parsed:
                    self.logger.info(f"✅ Successfully fetched from {source['name']}")
                    return {'source': source['name'], 'data': parsed}
        self.logger.error("❌ All orderbook sources failed")
        return None

    def fetch_price_data(self):
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {'ids': 'solana', 'vs_currencies': 'usd', 'include_24hr_vol': 'true', 'include_24hr_change': 'true'}
            data = self.fetch_with_retry(url, params, "CoinGecko")
            if data and 'solana' in data:
                return {'price': data['solana']['usd'], 'volume_24h': data['solana'].get('usd_24h_vol', 0), 'change_24h': data['solana'].get('usd_24h_change', 0)}
        except Exception as e:
            self.logger.error(f"Price data error: {e}")
        return None

class SOLMonitor:
    def __init__(self, email_config):
        self.setup_logging()
        self.email_config = email_config
        self.collector = DataCollector(self.logger)
        self.init_database()
        self.last_price = 0  

    def setup_logging(self):
        os.makedirs(os.path.dirname(Config.LOG_PATH), exist_ok=True)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(Config.LOG_PATH), logging.StreamHandler()])
        self.logger = logging.getLogger(__name__)

    def init_database(self):
        os.makedirs(os.path.dirname(Config.DB_PATH), exist_ok=True)
        conn = sqlite3.connect(Config.DB_PATH)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS market_data (timestamp INTEGER PRIMARY KEY, price REAL, volume_24h REAL, change_24h REAL, bid_depth REAL, ask_depth REAL, total_depth REAL, spread_bps REAL, source TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER, alert_type TEXT, message TEXT, value REAL, threshold REAL, email_sent INTEGER DEFAULT 0)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_timestamp ON market_data(timestamp DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp DESC)")
        conn.commit()
        conn.close()
        self.logger.info("Database initialized")

    def calculate_rsi(self, prices, period=14):
        """計算RSI指標"""
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

    def calculate_market_depth(self, orderbook_data):
        try:
            data, source = orderbook_data['data'], orderbook_data['source']
            bids = [(float(b[0]), float(b[1])) for b in data.get('bids', [])[:50]]
            asks = [(float(a[0]), float(a[1])) for a in data.get('asks', [])[:50]]
            if not bids or not asks: return None
            best_bid, best_ask = bids[0][0], asks[0][0]
            mid_price = (best_bid + best_ask) / 2
            self.last_price = mid_price 
            bid_threshold, ask_threshold = mid_price * 0.99, mid_price * 1.01
            bid_depth = sum(p * s for p, s in bids if p >= bid_threshold)
            ask_depth = sum(p * s for p, s in asks if p <= ask_threshold)
            total_depth = bid_depth + ask_depth
            depth_imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0  # 新增
            spread_bps = ((best_ask - best_bid) / mid_price) * 10000
            return {
                'price': mid_price, 
                'bid_depth': bid_depth, 
                'ask_depth': ask_depth, 
                'total_depth': total_depth, 
                'depth_imbalance': depth_imbalance,  
                'spread_bps': spread_bps, 
                'source': source
            }
        except Exception as e:
            self.logger.error(f"Depth calculation error: {e}")
            return None

    def calculate_vrp(self, df, change_24h):
        """VRP"""
        try:
            if len(df) < 24:
                return None
            
            # 已實現波動率
            prices = df['price'].tail(24).values
            returns = np.diff(np.log(prices))
            realized_vol = np.std(returns) * np.sqrt(365 * 24 * 12)  # 年化
            
            # 隱含波動率代理
            implied_vol = abs(change_24h) / 100 * np.sqrt(365)
            
            # VRP = IV - RV
            vrp = implied_vol - realized_vol
            
            return vrp
        except Exception as e:
            self.logger.error(f"VRP calculation error: {e}")
            return None

    def check_alerts(self, depth_data):
        try:
            conn = sqlite3.connect(Config.DB_PATH)
            cutoff = int(time.time()) - 30 * 24 * 3600
            df = pd.read_sql_query("SELECT * FROM market_data WHERE timestamp > ?", conn, params=(cutoff,))
            
            if len(df) >= 10:
                # 1. Market Depth
                mean, std = df['total_depth'].mean(), df['total_depth'].std()
                threshold = mean - Config.DEPTH_STD_THRESHOLD * std
                current = depth_data['total_depth']
                self.logger.info(f"Depth - Current: ${current:,.0f}, Mean: ${mean:,.0f}, Std: ${std:,.0f}, Threshold: ${threshold:,.0f}")
                
                if current < threshold:
                    self.send_alert('DEPTH_DECLINE', 
                                   f'Market depth ${current:,.0f} below threshold ${threshold:,.0f}', 
                                   current, threshold)
                
                # 2. VRP
                if 'change_24h' in depth_data:
                    vrp = self.calculate_vrp(df, depth_data['change_24h'])
                    if vrp is not None:
                        self.logger.info(f"VRP: {vrp*100:.2f}%")
                        if vrp < Config.VRP_MIN_THRESHOLD:
                            self.send_alert('LOW_VRP', 
                                          f'VRP {vrp*100:.2f}% below {Config.VRP_MIN_THRESHOLD*100}% threshold', 
                                          vrp, Config.VRP_MIN_THRESHOLD)
                
                # 3. RSI
                prices = df['price'].tail(15).values  
                if len(prices) >= 15:
                    rsi = self.calculate_rsi(prices, period=14)
                    if rsi is not None:
                        self.logger.info(f"RSI: {rsi:.1f}")
                        if rsi > 80: 
                            self.send_alert('RSI_OVERBOUGHT', 
                                          f'RSI {rsi:.1f} indicates overbought condition', 
                                          rsi, 80)
                        elif rsi < 20: 
                            self.send_alert('RSI_OVERSOLD', 
                                          f'RSI {rsi:.1f} indicates oversold condition', 
                                          rsi, 20)
            else:
                self.logger.info(f"Not enough data for statistics (only {len(df)} records)")

            # 4. Spread
            if depth_data.get('spread_bps', 0) > Config.SPREAD_MAX_BPS:
                self.send_alert('WIDE_SPREAD', 
                               f'Spread {depth_data["spread_bps"]:.2f} bps exceeds {Config.SPREAD_MAX_BPS} bps', 
                               depth_data['spread_bps'], Config.SPREAD_MAX_BPS)
            
            # 5. Order Book
            if 'depth_imbalance' in depth_data:
                imbalance = abs(depth_data['depth_imbalance'])
                self.logger.info(f"Depth Imbalance: {depth_data['depth_imbalance']*100:.1f}%")
                if imbalance > 0.3: 
                    side = "bid-heavy" if depth_data['depth_imbalance'] > 0 else "ask-heavy"
                    self.send_alert('DEPTH_IMBALANCE', 
                                   f'Order book is {imbalance*100:.1f}% {side}', 
                                   depth_data['depth_imbalance'], 0.3)
            
            conn.close()
        except Exception as e:
            self.logger.error(f"Alert check error: {e}")

    def send_alert(self, alert_type, message, value, threshold):
        try:
            conn = sqlite3.connect(Config.DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("SELECT MAX(timestamp) FROM alerts WHERE alert_type = ? AND email_sent = 1", (alert_type,))
            last_sent = cursor.fetchone()[0]
            current_time = int(time.time())
            
            cursor.execute("INSERT INTO alerts (timestamp, alert_type, message, value, threshold, email_sent) VALUES (?, ?, ?, ?, ?, ?)", 
                          (current_time, alert_type, message, value, threshold, 0))
            conn.commit()

            if last_sent and (current_time - last_sent) < Config.EMAIL_COOLDOWN:
                self.logger.info(f"Alert {alert_type} in cooldown period")
                conn.close()
                return
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"🚨 SOL Alert: {alert_type}"
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(self.email_config['recipients'])
            
            alert_config = {
                'DEPTH_DECLINE': {'color': '#e74c3c', 'icon': '📉', 'severity': 'HIGH'},
                'LOW_VRP': {'color': '#e67e22', 'icon': '⚡', 'severity': 'HIGH'},
                'RSI_OVERBOUGHT': {'color': '#f39c12', 'icon': '📈', 'severity': 'MEDIUM'},
                'RSI_OVERSOLD': {'color': '#3498db', 'icon': '📉', 'severity': 'MEDIUM'},
                'WIDE_SPREAD': {'color': '#9b59b6', 'icon': '⚠️', 'severity': 'MEDIUM'},
                'DEPTH_IMBALANCE': {'color': '#95a5a6', 'icon': '⚖️', 'severity': 'LOW'},
                'TEST_ALERT': {'color': '#27ae60', 'icon': '✅', 'severity': 'INFO'}
            }
            
            config = alert_config.get(alert_type, {'color': '#e74c3c', 'icon': '🚨', 'severity': 'UNKNOWN'})
            
            html = f"""
            <html>
            <body style="font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5;">
                <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                    <div style="background: {config['color']}; color: white; padding: 20px;">
                        <h2 style="margin: 0;">{config['icon']} SOL Risk Alert</h2>
                        <p style="margin: 10px 0 0 0; opacity: 0.9;">{alert_type.replace('_', ' ')} - {config['severity']}</p>
                    </div>
                    
                    <div style="padding: 20px;">
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                            <h3 style="margin-top: 0; color: #495057;">Alert Details</h3>
                            <p><strong>Message:</strong> {message}</p>
                            <p><strong>Current Value:</strong> {value:.4f}</p>
                            <p><strong>Threshold:</strong> {threshold:.4f}</p>
                            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
                        </div>
                        
                        <div style="background: #fff3cd; padding: 15px; border-radius: 5px; border-left: 4px solid #ffc107;">
                            <h3 style="margin-top: 0; color: #856404;">⚠️ Recommended Actions</h3>
                            <ul style="margin: 10px 0; padding-left: 20px; color: #856404;">
                                {"<li>Consider taking profits - market may be overheated</li>" if alert_type == "RSI_OVERBOUGHT" else ""}
                                {"<li>Potential buying opportunity - market may be oversold</li>" if alert_type == "RSI_OVERSOLD" else ""}
                                {"<li>Order flow is heavily imbalanced - expect volatility</li>" if alert_type == "DEPTH_IMBALANCE" else ""}
                                <li>Reduce leverage to below 10x immediately</li>
                                <li>Switch to limit orders only</li>
                                <li>Monitor liquidation levels closely</li>
                                <li>Check for any major news affecting SOL</li>
                            </ul>
                        </div>
                        
                        <div style="margin-top: 20px; padding: 15px; background: #d1ecf1; border-radius: 5px;">
                            <h3 style="margin-top: 0; color: #0c5460;">📊 Current Market Status</h3>
                            <p style="margin: 5px 0;"><strong>SOL Price:</strong> ${self.last_price:.2f}</p>
                            <p style="margin: 5px 0;"><strong>Alert Type:</strong> {alert_type}</p>
                            <p style="margin: 5px 0;"><strong>Severity:</strong> {config['severity']}</p>
                        </div>
                    </div>
                    
                    <div style="background: #f8f9fa; padding: 15px; text-align: center; font-size: 12px; color: #6c757d;">
                        SOL Risk Monitor • Cloud Run • Next {alert_type} alert in 30+ minutes
                    </div>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html, 'html'))
            
            try:
                server = smtplib.SMTP('smtp.gmail.com', 587)
                server.starttls()
                server.login(self.email_config['sender_email'], self.email_config['sender_password'])
                text = msg.as_string()
                server.sendmail(self.email_config['sender_email'], self.email_config['recipients'], text)
                server.quit()
                
                cursor.execute("UPDATE alerts SET email_sent = 1 WHERE timestamp = ? AND alert_type = ?", (current_time, alert_type))
                conn.commit()
                self.logger.info(f"✅ Alert email sent: {alert_type}")
                
            except Exception as smtp_error:
                self.logger.error(f"SMTP error: {smtp_error}")
                self.logger.error(f"Check if app password is correct and 2FA is enabled")
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Alert sending error: {e}")
    
    def run_cycle(self):
        try:
            self.logger.info("="*50 + "\nStarting monitoring cycle...")
            orderbook = self.collector.fetch_orderbook_any_source()
            if not orderbook: 
                self.logger.warning("No orderbook data available")
                return {"status": "error", "message": "No orderbook data"}
            
            depth_data = self.calculate_market_depth(orderbook)
            if not depth_data: 
                self.logger.warning("Failed to calculate market depth")
                return {"status": "error", "message": "Failed to calculate depth"}
            
            price_data = self.collector.fetch_price_data()
            if price_data:
                depth_data.update(price_data)

            conn = sqlite3.connect(Config.DB_PATH)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO market_data (timestamp, price, volume_24h, change_24h, bid_depth, ask_depth, total_depth, spread_bps, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                          (int(time.time()), depth_data['price'], depth_data.get('volume_24h', 0), depth_data.get('change_24h', 0), 
                           depth_data['bid_depth'], depth_data['ask_depth'], depth_data['total_depth'], depth_data['spread_bps'], depth_data['source']))
            conn.commit()
            conn.close()
            
            self.check_alerts(depth_data)
            
            self.logger.info(f"✅ Cycle complete. Source: {depth_data['source']}, Price: ${depth_data['price']:.2f}, Depth: ${depth_data['total_depth']:,.0f}")
            
            return {
                "status": "success",
                "data": {
                    "price": depth_data['price'],
                    "total_depth": depth_data['total_depth'],
                    "spread_bps": depth_data['spread_bps'],
                    "source": depth_data['source']
                }
            }
            
        except Exception as e:
            self.logger.error(f"Cycle error: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

app = Flask(__name__)
monitor = None

def initialize_monitor():
    global monitor
    if monitor is None:
        print("🔧 Initializing monitor...")
        email_config = {
            'sender_email': os.environ.get('SENDER_EMAIL', 'abbysuyuyan.python@gmail.com'),
            'sender_password': os.environ.get('SENDER_PASSWORD'), 
            'recipients': os.environ.get('RECIPIENTS', 'abbysuyuyan@gmail.com').split(',')
        }
        
        if not email_config['sender_password']:
            raise ValueError("Missing SENDER_PASSWORD environment variable (Gmail app password)")
        
        email_config['recipients'] = [email.strip() for email in email_config['recipients']]
        monitor = SOLMonitor(email_config)
        print("✅ Monitor initialized successfully!")
    return monitor

@app.route("/", methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "SOL Risk Monitor",
        "timestamp": datetime.now().isoformat()
    })

@app.route("/run", methods=['POST', 'GET'])
def execute_task():
    try:
        current_monitor = initialize_monitor()
        result = current_monitor.run_cycle()
        return jsonify(result), 200 if result["status"] == "success" else 500
    except Exception as e:
        logging.error(f"Error in execute_task: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/test-email", methods=['GET'])
def test_email():
    try:
        current_monitor = initialize_monitor()
        current_monitor.send_alert(
            'TEST_ALERT', 
            'This is a test alert to verify email configuration', 
            123.45, 
            100.00
        )
        return jsonify({"status": "success", "message": "Test email sent"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    initialize_monitor()
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
