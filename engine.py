import json
import sqlite3
from datetime import datetime
from confluent_kafka import Consumer

# --- 1. THE DATABASE LAYER ---
class SQLiteAlertSink:
    def __init__(self, db_name="alerts.sqlite3"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                value REAL,
                z_score REAL,
                mean REAL,
                std_dev REAL
            )
        ''')
        self.conn.commit()

    def save_anomaly(self, val, z, m, s):
        now = datetime.now().isoformat()
        self.cursor.execute('''
            INSERT INTO anomalies (timestamp, value, z_score, mean, std_dev)
            VALUES (?, ?, ?, ?, ?)
        ''', (now, val, z, m, s))
        self.conn.commit()
        print(f"Logged to DB: Z={z:.2f}")

# --- 2. THE STATS ENGINE ---
class WelfordEngine:
    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0

    def update(self, x):
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.M2 += delta * delta2

    def get_stats(self):
        if self.n < 2: return self.mean, 0.0
        return self.mean, (self.M2 / (self.n - 1))**0.5

# --- 3. THE MAIN LOOP ---
db_sink = SQLiteAlertSink()
stats = WelfordEngine()
THRESHOLD = 3.0

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'spc-engine-v2',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['sensor-data'])

print("Engine Live. Monitoring & Archiving...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        data = json.loads(msg.value().decode('utf-8'))
        val = data['reading']
        
        curr_mean, curr_std = stats.get_stats()
        
        if curr_std > 0:
            z_score = abs(val - curr_mean) / curr_std
            if z_score > THRESHOLD:
                print(f"⚠️ ANOMALY! Val: {val:.2f}")
                # SAVE TO DATABASE
                db_sink.save_anomaly(val, z_score, curr_mean, curr_std)
        
        stats.update(val)
finally:
    consumer.close()