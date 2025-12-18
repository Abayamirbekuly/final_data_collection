import json
import sqlite3
import pandas as pd
from kafka import KafkaConsumer

def run_cleaner():
    consumer = KafkaConsumer(
        'raw_data_topic',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000,
        api_version=(0, 10, 1)
    )

    db_path = '/opt/airflow/data/app.db'
    conn = sqlite3.connect(db_path)
    
    messages_found = False
    for message in consumer:
        messages_found = True
        data = message.value
        price = data['bpi']['USD']['rate_float']
        ts = data['time']['updated']
        
        df = pd.DataFrame([{'price': price, 'timestamp': ts}])
        df.to_sql('cleaned_data', conn, if_exists='append', index=False)
        print(f"Cleaned: {price}")
    
    conn.close()
    if not messages_found:
        print("No new messages in Kafka")

if __name__ == "__main__":
    run_cleaner()