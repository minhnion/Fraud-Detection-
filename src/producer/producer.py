import pandas as pd
from confluent_kafka import Producer
import json
import time
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

config = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(config)

topic = 'transactions'

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} partition [{msg.partition()}] at offset {msg.offset()}')

def send_data_from_csv(file_path):
    try:
        df = pd.read_csv(file_path)

        for index, row in df.iterrows():
            record = row.to_dict()
            record_value = json.dumps(record)

            producer.produce(topic, value=record_value.encode('utf-8'), callback=delivery_report)
            producer.poll(0)
            time.sleep(0.01)
        logger.info("All messages have been sent.")
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        producer.flush()

if __name__ == "__main__":
    csv_file_path = 'data/creditcard.csv'
    send_data_from_csv(csv_file_path)
    