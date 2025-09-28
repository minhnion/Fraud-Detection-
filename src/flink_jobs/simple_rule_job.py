import json 
from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer

def fraud_detection_job():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(5000)  

    # Configure Kafka consumer
    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_topics('transactions') \
        .set_group_id('flink-fraud-detector') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Configure Kafka producer
    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic('fraud_alerts')
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    source_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Transactions Source")

    def parse_json(record_str):
        try:
            return json.loads(record_str)
        except:
            return None
        
    def is_fraudulent(record):
        return record is not None and 'Amount' in record and record['Amount'] > 2000
    
    fraudulent_transactions = source_stream \
        .map(parse_json, output_type=Types.PICKLED_BYTE_ARRAY()) \
        .filter(is_fraudulent) \
        .map(json.dumps, output_type=Types.STRING())
    
    # Send fraudulent transactions to Kafka sink
    fraudulent_transactions.sink_to(sink).name("Kafka Fraud Alerts Sink")

    # Execute the Flink job
    env.execute("Fraud Detection Job")
    print("Flink job executed successfully.")

if __name__ == '__main__':
    fraud_detection_job()