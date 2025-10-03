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

### Bước 2: Huấn luyện Mô hình Machine Learning

Chạy script để huấn luyện và lưu mô hình.  
Mô hình này sẽ được lưu dưới dạng file `.pkl` và được Flink job tải lên từ MinIO để dự đoán.

```bash
python train_sklearn_model.py
```
Sau khi chạy, bạn sẽ thấy file sklearn_fraud_model.pkl được tạo trong thư mục data/model/.

### Bước 3: Khởi động Hạ tầng Big Data

Sử dụng **Docker Compose** để xây dựng Flink image và khởi động toàn bộ các dịch vụ (Kafka, Flink, MinIO):

```bash
docker-compose up --build -d
```
Trong đó:
Tùy chọn --build đảm bảo Flink image được rebuild với các thư viện ML đã khai báo trong Dockerfile. Tuỳ chọn -d chạy các container ở chế độ nền (detached mode).

### Bước 4: Chuẩn bị MinIO và Kafka Topics

#### 1. Tạo MinIO Bucket
- Mở trình duyệt và truy cập: [http://localhost:9001](http://localhost:9001)  
- Đăng nhập với tài khoản mặc định:
  - **User:** `minioadmin`
  - **Password:** `minioadmin`  
- Nhấn **"Create Bucket"** và tạo một bucket mới tên là **`datalake`**

#### 2. (Tùy chọn nhưng khuyến khích) Tạo Kafka Topics
Chạy các lệnh sau để tạo sẵn topic, tránh lỗi *"Unknown Topic"* khi Flink job chạy:

```bash
docker exec -it kafka kafka-topics --create --topic transactions --bootstrap-server kafka:29092
docker exec -it kafka kafka-topics --create --topic fraud_alerts --bootstrap-server kafka:29092
```

### Bước 5: Chạy Pipeline Hoàn chỉnh

Bạn sẽ cần mở **3 cửa sổ terminal riêng biệt** để khởi động toàn bộ pipeline.

---

#### 🖥️ Terminal 1 – Submit Flink Jobs
Nộp cả hai Flink job để bắt đầu xử lý dữ liệu:

```bash
# Job 1: Dùng ML để phát hiện gian lận
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /app/flink_jobs/ml_inference_job.py

# Job 2: Lưu trữ dữ liệu thô vào MinIO
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /app/flink_jobs/kafka_to_minio_job.py
```
📌 Kiểm tra: Truy cập Flink UI tại http://localhost:8081

### 🖥️ Terminal 2 – Chạy Consumer để nhận Cảnh báo

Consumer script sẽ lắng nghe và in ra các cảnh báo gian lận mà Flink phát hiện:

```bash
python src/consumer/consumer.py
```
(Terminal này sẽ luôn chờ để hiển thị tin nhắn từ Kafka.)

### 🖥️ Terminal 3 – Chạy Producer để gửi Dữ liệ
Producer script bắt đầu phát dữ liệu giao dịch vào Kafka:
```bash
python src/producer/producer.py
```
👉 Đây là bước cuối cùng để kích hoạt toàn bộ luồng dữ liệu.

### Bước 6: Quan sát Kết quả

- **Terminal 2 (Consumer):**  
  Bạn sẽ thấy các bản ghi giao dịch đầy đủ được in ra màn hình mỗi khi mô hình ML dự đoán một giao dịch là **gian lận**.

- **MinIO UI ([http://localhost:9001](http://localhost:9001)):**  
  Sau vài phút, truy cập bucket **`datalake`** → bạn sẽ thấy thư mục **`raw/transactions/`** được tạo, trong đó chứa các tệp dữ liệu giao dịch thô mà Flink đã ghi vào.

---

### Bước 7: Dọn dẹp

Để dừng và xóa toàn bộ các container đang chạy, dùng lệnh:

```bash
docker-compose down
```
