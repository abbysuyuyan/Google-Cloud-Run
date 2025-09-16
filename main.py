import os
import time
import json
import base64
import sqlite3
import requests
import logging
import smtplib
import pandas as pd
import numpy as np
from datetime import datetime
from flask import Flask
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --- Configuration Class ---
class Config:
    DEPTH_STD_THRESHOLD = 1.0
    VRP_MIN_THRESHOLD = 0.01
    SPREAD_MAX_BPS = 20
    EMAIL_COOLDOWN = 1800  # 30分鐘
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

# --- SOLMonitor Class ---
# (這是您提供的程式碼，移除了 Colab 特有功能)
class SOLMonitor:
    def __init__(self, email_config):
        self.setup_logging()
        self.email_config = email_config
        self.collector = DataCollector(self.logger)
        self.init_database()

    def setup_logging(self):
        # 確保 log 檔案的目錄存在
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

    def calculate_market_depth(self, orderbook_data):
        try:
            data, source = orderbook_data['data'], orderbook_data['source']
            bids = [(float(b[0]), float(b[1])) for b in data.get('bids', [])[:50]]
            asks = [(float(a[0]), float(a[1])) for a in data.get('asks', [])[:50]]
            if not bids or not asks: return None
            best_bid, best_ask = bids[0][0], asks[0][0]
            mid_price = (best_bid + best_ask) / 2
            bid_threshold, ask_threshold = mid_price * 0.99, mid_price * 1.01
            bid_depth = sum(p * s for p, s in bids if p >= bid_threshold)
            ask_depth = sum(p * s for p, s in asks if p <= ask_threshold)
            spread_bps = ((best_ask - best_bid) / mid_price) * 10000
            return {'price': mid_price, 'bid_depth': bid_depth, 'ask_depth': ask_depth, 'total_depth': bid_depth + ask_depth, 'spread_bps': spread_bps, 'source': source}
        except Exception as e:
            self.logger.error(f"Depth calculation error: {e}")
            return None

    def check_alerts(self, depth_data):
        try:
            conn = sqlite3.connect(Config.DB_PATH)
            cutoff = int(time.time()) - 30 * 24 * 3600
            df = pd.read_sql_query("SELECT total_depth FROM market_data WHERE timestamp > ?", conn, params=(cutoff,))
            if len(df) >= 10:
                mean, std = df['total_depth'].mean(), df['total_depth'].std()
                threshold = mean - Config.DEPTH_STD_THRESHOLD * std
                current = depth_data['total_depth']
                self.logger.info(f"Depth - Current: ${current:,.0f}, Mean: ${mean:,.0f}, Std: ${std:,.0f}, Threshold: ${threshold:,.0f}")
                if current < threshold:
                    self.send_alert('DEPTH_DECLINE', f'Market depth ${current:,.0f} below threshold ${threshold:,.0f}', current, threshold)
            else:
                self.logger.info(f"Not enough data for statistics (only {len(df)} records)")

            if depth_data.get('spread_bps', 0) > Config.SPREAD_MAX_BPS:
                self.send_alert('WIDE_SPREAD', f'Spread {depth_data["spread_bps"]:.2f} bps exceeds {Config.SPREAD_MAX_BPS} bps', depth_data['spread_bps'], Config.SPREAD_MAX_BPS)
            
            # ... (VRP check logic can be added here if needed, keeping it concise for now) ...
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
            cursor.execute("INSERT INTO alerts (timestamp, alert_type, message, value, threshold) VALUES (?, ?, ?, ?, ?)", (current_time, alert_type, message, value, threshold))
            conn.commit()

            if last_sent and (current_time - last_sent) < Config.EMAIL_COOLDOWN:
                self.logger.info(f"Alert {alert_type} in cooldown period")
                conn.close()
                return
            
            # ... (Email sending logic, using self.email_config) ...
            msg = MIMEMultipart()
            msg['Subject'], msg['From'], msg['To'] = f"🚨 SOL Alert: {alert_type}", self.email_config['sender_email'], ', '.join(self.email_config['recipients'])
            html = f"<html><body><h2>🚨 SOL Risk Alert: {alert_type}</h2><p>{message}</p></body></html>" # Simplified HTML
            msg.attach(MIMEText(html, 'html'))
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            server.send_message(msg)
            server.quit()

            cursor.execute("UPDATE alerts SET email_sent = 1 WHERE timestamp = ? AND alert_type = ?", (current_time, alert_type))
            conn.commit()
            self.logger.info(f"✅ Alert email sent: {alert_type}")

        except Exception as e:
            self.logger.error(f"Alert sending error: {e}")
        finally:
            if conn: conn.close()
    
    def run_cycle(self):
        try:
            self.logger.info("="*50 + "\nStarting monitoring cycle...")
            orderbook = self.collector.fetch_orderbook_any_source()
            if not orderbook: return self.logger.warning("No orderbook data available")
            depth_data = self.calculate_market_depth(orderbook)
            if not depth_data: return self.logger.warning("Failed to calculate market depth")
            price_data = self.collector.fetch_price_data()
            if price_data:
                depth_data.update(price_data)

            conn = sqlite3.connect(Config.DB_PATH)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO market_data (timestamp, price, volume_24h, change_24h, bid_depth, ask_depth, total_depth, spread_bps, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (int(time.time()), depth_data['price'], depth_data.get('volume_24h', 0), depth_data.get('change_24h', 0), depth_data['bid_depth'], depth_data['ask_depth'], depth_data['total_depth'], depth_data['spread_bps'], depth_data['source']))
            conn.commit()
            conn.close()
            self.check_alerts(depth_data)
            self.logger.info(f"✅ Cycle complete. Source: {depth_data['source']}, Price: ${depth_data['price']:.2f}, Depth: ${depth_data['total_depth']:,.0f}")
        except Exception as e:
            self.logger.error(f"Cycle error: {e}", exc_info=True)

# --- Flask App & Global Monitor Instance ---
app = Flask(__name__)
monitor = None

def initialize_monitor():
    """從環境變數載入設定並初始化監控器"""
    global monitor
    if monitor is None:
        print("🔧 Initializing monitor for the first time...")
        email_config = {
            'sender_email': os.environ.get('SENDER_EMAIL'),
            'sender_password': os.environ.get('SENDER_PASSWORD'),
            'recipients': os.environ.get('RECIPIENTS', '').split(',')
        }
        if not all([email_config['sender_email'], email_config['sender_password'], email_config['recipients']]):
            raise ValueError("Missing required environment variables: SENDER_EMAIL, SENDER_PASSWORD, RECIPIENTS")
        
        email_config['recipients'] = [email.strip() for email in email_config['recipients']]
        monitor = SOLMonitor(email_config)
        print("✅ Monitor initialized successfully!")
    return monitor

@app.route("/run", methods=['POST'])
def execute_task():
    """這是由 n8n 呼叫的 API 端點，用來觸發一次監控循環"""
    try:
        current_monitor = initialize_monitor()
        current_monitor.run_cycle()
        return ("Monitoring cycle executed successfully.", 200)
    except Exception as e:
        logging.error(f"Error in execute_task: {e}", exc_info=True)
        return ("An error occurred during execution.", 500)

if __name__ == "__main__":
    initialize_monitor()
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
