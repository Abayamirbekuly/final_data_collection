import json
import requests
from kafka import KafkaProducer
import time

def run_producer():
    # Используем внутренний адрес сети Docker
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        producer.send('raw_data_topic', data)
        producer.flush()
        print("Success: Data sent to Kafka")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()