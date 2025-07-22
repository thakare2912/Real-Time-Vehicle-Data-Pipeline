# Real-Time Vehicle Data Pipeline

This project demonstrates an **end-to-end real-time data engineering pipeline** using:
- **Apache Kafka** for event streaming
- **MinIO (S3)** as the landing zone for raw & processed data
- **Apache Spark Structured Streaming** for ETL and aggregations
- **Snowflake** as the cloud data warehouse for analytics-ready data

---

##  **Key Features**

- **Kafka Producer:** Simulates live vehicle sensor data including speed, location, fines.
- **Kafka Consumer:** Consumes events in batches and writes partitioned CSVs to MinIO.
- **Spark Structured Streaming:** Reads raw data from MinIO, parses timestamps, partitions data by year/month/day, adds aggregations (e.g., `total_fine` per user) and writes to processed zone.
- **Snowflake Loader:** Incrementally loads processed Parquet data from MinIO to Snowflake using a MERGE strategy (upserts).
---

##  **Tech Stack**

- **Python**: Data generation, Kafka producer/consumer, Snowflake loader
- **Apache Kafka**: Event streaming backbone
- **MinIO (S3)**: Data lake storage for raw & processed data
- **Apache Spark Structured Streaming**: ETL, window aggregations
- **Snowflake**: Cloud data warehouse

---

## **How it works**

1️) **Producer:** `vehicle_producer.py` continuously streams random vehicle events to Kafka.  
2️) **Consumer:** `batch_consumer.py` reads Kafka topic, buffers messages, and flushes CSV files to MinIO every few seconds or batch size.  
3️) **Spark Processor:** `streaming_job.py` reads raw CSVs from MinIO, adds `event_time`, `year`, `month`, `day`, computes `total_fine` per `user_id`, writes clean Parquet data back to MinIO.  
4️) **Snowflake Loader:** `snowflake_loader.py` scans processed files in MinIO, merges data into Snowflake using `MERGE` for upserts.

---

##  **Key Learning**

- Real-time streaming using Kafka + Spark
- Partitioning and event-time handling in Spark Structured Streaming
- Working with MinIO as S3-compatible storage
- Batch ingestion and MERGE into Snowflake

---

##  **Run it**

- `pip install -r requirements.txt`
- Run Kafka + MinIO (or use `docker-compose.yml`)
- Start the producer: `python producer/vehicle_producer.py`
- Start the consumer: `python consumer/batch_consumer.py`
- Start Spark job:
-                    `docker exec -it stockmarketdatapipeline-spark-master-1 bash`
-                   `spark-submit \
                    --master spark://spark-master:7077 \
                    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
                     /opt/spark/jobs/vehicle-info_PySpark_Streaming.py`
- Load to Snowflake: `python snowflake_loader/snowflake_loader.py`

---

##  **Author**

Built by [HANUMANT NANASAHEB THAKARE] — Data Engineering Portfolio 

---


