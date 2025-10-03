import json
import pandas as pd
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer

class FraudPredictor(MapFunction):
    def __init__(self):
        self.model = None
        self.feature_columns = [f'V{i}' for i in range(1, 29)] + ['Amount']

    def open(self, runtime_context: RuntimeContext):

        import joblib
        model_path = '/app/data/model/sklearn_fraud_model.pkl'
        self.model = joblib.load(model_path)

    def map(self, value):
        try:
            record = json.loads(value)
            
            features_df = pd.DataFrame([record], columns=self.feature_columns)
            
            prediction = self.model.predict(features_df)
            
            if prediction[0] == 1:
                record['is_fraud'] = True
                return json.dumps(record)
            else:
                return None
        except Exception as e:
            return None

def ml_fraud_detection_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_cached_file('/app/data/model/sklearn_fraud_model.pkl', 'sklearn_fraud_model.pkl', False)
    
    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_topics('transactions') \
        .set_group_id('flink-ml-fraud-detector') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic('fraud_alerts')
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    source_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    alerts_stream = source_stream \
        .map(FraudPredictor(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
        
    alerts_stream.print() 
    alerts_stream.sink_to(sink).name("Kafka Fraud Alerts Sink")
    
    env.execute("ML Fraud Detection Job")

if __name__ == '__main__':
    ml_fraud_detection_job()