import time
import json
import random
from confluent_kafka import Producer

# Kafka configuration
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err: print(f'Message delivery failed: {err}')

while True:
    # Normally distributed data (mean=50, std=5)
    val = random.normalvariate(50, 5)
    
    # 2% chance of a "Breaking" anomaly
    if random.random() < 0.02:
        val = val * 3  # A massive spike
        
    data = {"timestamp": time.time(), "reading": val}
    
    producer.produce('sensor-data', json.dumps(data), callback=delivery_report)
    producer.poll(0)
    
    print(f"Sent: {val:.2f}")
    time.sleep(0.5) # Send 2 readings per second