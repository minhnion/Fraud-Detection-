import os 
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common.serialization import Encoder
import json

# Define the schema for the CSV data
ROW_TYPE_INFO = Types.ROW_NAMED(
    ["Time", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10",
     "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20",
     "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount", "Class"],
    [Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.INT()]
)

def kafka_to_minio_job():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60000)

    # Define Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_topics('transactions') \
        .set_group_id('flink-lake-writer-group') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    print("Starting Kafka to MinIO Job...")

    source_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    source_stream.print() 

    def to_row(json_string):
        
        print(f"MAP - Received JSON string: {json_string}")
        
        try:
            data = json.loads(json_string)
            field_names = ROW_TYPE_INFO.get_field_names()
            
            row_data = {}
            for field in field_names:
                if field == 'Class':
                    row_data[field] = int(data.get(field, 0))
                else:
                    row_data[field] = data.get(field)

            result_row = Row(**row_data)

            print(f"MAP - Successfully created Row: {result_row}")
            return result_row
        except Exception as e:
            print(f"MAP - FAILED to process record. Error: {e}, Record: {json_string}")
            return None 
    
    row_stream = source_stream.map(to_row, output_type=ROW_TYPE_INFO).filter(lambda r: r is not None)


    output_path = "s3a://datalake/raw/transactions/"

    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder("UTF-8")) \
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("tx")
            .with_part_suffix(".parquet")
            .build()) \
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(
                part_size=1024 ** 3, 
                rollover_interval=15 * 60 * 1000, 
                inactivity_interval=5 * 60 * 1000
                )) \
        .build()
    
    row_stream.sink_to(file_sink)

    env.execute("Kafka to MinIO Data Lake Job")

if __name__ == '__main__':
    kafka_to_minio_job()