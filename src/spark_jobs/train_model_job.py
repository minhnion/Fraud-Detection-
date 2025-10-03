import os
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

def train_fraud_detection_model():
    S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
    S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    spark = SparkSession.builder \
        .appName("Fraud Detection Model Training") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark Session created successfully.")

    input_path = "s3a://datalake/raw/transactions/"
    try:
        df_raw = spark.read.csv("s3a://datalake/creditcard.csv", header=True, inferSchema=True)
        print(f"Successfully read data from {input_path}")
    except Exception as e:
        print(f"Failed to read data from MinIO. Error: {e}")
        print("Reading local sample CSV file for demo purposes...")
        df_raw = spark.read.csv("/app/data/creditcard.csv", header=True, inferSchema=True)


    df = df_raw.withColumn("Class", col("Class").cast(DoubleType()))

    feature_cols = [c for c in df.columns if c not in ['Time', 'Class']]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="assembled_features")
    
    scaler = StandardScaler(inputCol="assembled_features", outputCol="features")

    print("Feature engineering steps defined.")

    rf = RandomForestClassifier(labelCol="Class", featuresCol="features")
    
    print("Model (RandomForest) defined.")

    pipeline = Pipeline(stages=[assembler, scaler, rf])

    print("Training pipeline is starting...")
    model = pipeline.fit(df)
    print("Pipeline training completed.")

    output_model_path = "s3a://datalake/models/fraud_detection_model"
    model.write().overwrite().save(output_model_path)

    print(f"Model successfully saved to {output_model_path}")

    spark.stop()

if __name__ == "__main__":
    train_fraud_detection_model()