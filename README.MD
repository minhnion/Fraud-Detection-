# Real-Time Fraud Detection System

This project is a **real-time credit card fraud detection system** built on a modern Big Data architecture, leveraging **Apache Kafka, Apache Flink, and MinIO**.  
The system is capable of processing a continuous stream of transactions, applying a **Machine Learning model** to detect fraud in real-time, while also persisting all raw data for future analysis and model retraining.

---

## ✨ Key Features

- **Stream Processing**: Handles thousands of transactions per second in real-time.  
- **Fraud Detection with ML**: Integrates a **Scikit-learn Machine Learning model (Random Forest)** for more accurate fraud classification compared to static rules.  
- **Persistent Storage (Data Lake)**: Stores all raw transactions in **MinIO (S3-compatible)** for safe long-term storage, retraining, or batch analytics.  
- **Microservices & Containerization**: Each component is containerized with **Docker** and orchestrated with **Docker Compose**, simplifying deployment and scalability.  
- **Monitoring & Alerts**: Provides a dedicated alert channel for transactions flagged as potentially fraudulent.  

---

## 🏛️ System Architecture

The system is composed of the following core components:

- **Apache Kafka**: Acts as the *central nervous system* (message bus).  
  All transactions from the data source are pushed into Kafka, ensuring a fault-tolerant, durable, and decoupled data stream between producers and consumers.

- **Apache Flink**: Serves as the *real-time processing brain*.  
  Flink consumes data from Kafka and performs two parallel tasks:  
  - **Inference Stream**: Applies the trained ML model to predict fraud for each transaction.  
  - **Storage Stream**: Writes a copy of all raw transactions to the Data Lake.  

- **MinIO**: An **S3-compatible object storage system**, serving as the Data Lake to store raw transaction data and trained ML models.  

- **Scikit-learn**: Used to train the fraud detection model (**Random Forest**) from historical data.  
  The trained model is later loaded by Flink for real-time inference.  

- **Docker & Docker Compose**: Package all components (Kafka, Flink, MinIO) into isolated containers and define their interactions, making environment setup and scaling straightforward.  

---

## 🌊 Pipeline Workflow

The following diagram illustrates the flow of data through the system,  
including both the **offline training pipeline** and the **real-time prediction pipeline**.

```text
Offline Training (one-time)
---------------------------
[CSV Dataset] 
      │
      ▼
train_sklearn_model.py
      │
      ▼
 [Model.pkl]
      │
      ▼
[MinIO Data Lake]


Online Prediction (real-time)
-----------------------------
[Producer] ---> (Kafka Topic: transactions) ---> [Flink Job: ml_inference_job.py]
                                                       │
                                                       │ (load trained model from MinIO at startup)
                                                       │
                        ┌──────────────────────────────┴───────────────────────────────┐
                        │                                                              │
                Fraud Prediction                                                 Store Raw Data
                        │                                                              │
                        ▼                                                              ▼
        (Kafka Topic: fraud_alerts)                                      [Flink Job: kafka_to_minio_job.py]
                        │                                                              │
                        ▼                                                              ▼
             [Consumer / Alerting System]                                   [MinIO Data Lake]

```


## 🚀 How to Run the Project

### Requirements
- Docker and Docker Compose
- Python 3.8+ and pip
- Python virtual environment (recommended)

---

### Step 1: Initial Setup

Clone the repository:
```bash
git clone <your-repo-url>
cd fraud-detection
```
Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
Install Python dependencies:
```bash
pip install -r requirements.txt
```

Some required JAR files for Spark–AWS integration are not included in this repository (due to GitHub’s file size limit).  
You need to manually download them and place them into the `spark-jars/` directory.

Create the `spark-jars` directory
```bash
mkdir -p spark-jars
cd spark-jars
```
Download the required JAR files
```bash
# AWS Java SDK Bundle
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Hadoop AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
```

Verify

Make sure the spark-jars/ directory contains the following files:
```bash
spark-jars/
├── aws-java-sdk-bundle-1.12.262.jar
└── hadoop-aws-3.3.4.jar
```
### Step 2: Train Machine Learning Model

Run the script to train and save the model.  
This model will be saved as a `.pkl` file and loaded by the Flink job from MinIO for prediction.

```bash
python train_sklearn_model.py
```
After running, you will see the file sklearn_fraud_model.pkl created in the data/model/ directory.

### Step 3: Start Big Data Infrastructure

Use **Docker Compose** to build the Flink image and start all services (Kafka, Flink, MinIO):

```bash
docker-compose up --build -d
```
In which:
The --build option ensures the Flink image is rebuilt with the ML libraries declared in the Dockerfile.
The -d option runs the containers in detached mode (background).

### Step 4: Prepare MinIO and Kafka Topics

#### 1. Create MinIO Bucket
- Open the browser and go to: [http://localhost:9001](http://localhost:9001)  
- Log in with the default account:
  - **User:** `minioadmin`
  - **Password:** `minioadmin`  
- Click **"Create Bucket"** and create a new bucket named **`datalake`**

#### 2. (Optional but recommended) Create Kafka Topics
Run the following commands to pre-create topics and avoid *"Unknown Topic"* errors when the Flink job runs:

```bash
docker exec -it kafka kafka-topics --create --topic transactions --bootstrap-server kafka:29092
docker exec -it kafka kafka-topics --create --topic fraud_alerts --bootstrap-server kafka:29092
```

### Step 5: Run the Complete Pipeline

You will need to open **3 separate terminal windows** to start the entire pipeline.

---

#### 🖥️ Terminal 1 – Submit Flink Jobs
Submit both Flink jobs to start data processing:

```bash
# Job 1: Use ML to detect fraud
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /app/flink_jobs/ml_inference_job.py

# Job 2: Store raw data into MinIO
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /app/flink_jobs/kafka_to_minio_job.py
```
📌 Check: Access Flink UI at http://localhost:8081

### 🖥️ 🖥️ Terminal 2 – Run Consumer to Receive Alerts

The consumer script will listen and print fraud alerts detected by Flink:

```bash
python src/consumer/consumer.py
```
(This terminal will keep waiting to display messages from Kafka.)

### 🖥️ Terminal 3 – Run Producer to Send Data
The producer script starts sending transaction data into Kafka:
```bash
python src/producer/producer.py
```
👉 This is the final step to activate the entire data flow.

### Step 6: Observe the Results

- **Terminal 2 (Consumer):**  
  You will see full transaction records printed on the screen whenever the ML model predicts a transaction as **fraudulent**.

- **MinIO UI ([http://localhost:9001](http://localhost:9001)):**  
  After a few minutes, access the **`datalake`** bucket → you will see the **`raw/transactions/`** directory created, containing the raw transaction data files written by Flink.

---

### Step 7: Cleanup

To stop and remove all running containers, use the command:

```bash
docker-compose down
```
