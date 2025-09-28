from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transaction-consumer-group',
    'auto.offset.reset': 'earliest'
}

topic = 'transactions'

def consume_messages():
    logger.info("Starting consumer...")
    consumer = Consumer(config)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                record_value = msg.value().decode('utf-8')
                try:
                    data = json.loads(record_value)
                    logger.info(f"Consumed record: {data}")
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    consume_messages()