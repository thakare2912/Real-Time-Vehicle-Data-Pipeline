# Real-Time Vehicle Data Pipeline

This project demonstrates an **end-to-end real-time data engineering pipeline** using:
- **Apache Kafka** for event streaming
- **MinIO (S3)** as the landing zone for raw & processed data
- **Apache Spark Structured Streaming** for ETL and aggregations
- **Snowflake** as the cloud data warehouse for analytics-ready data
- **Apache Airflow** to automate Snowflake loads

---

##  **Key Features**

- **Kafka Producer:** Simulates live vehicle sensor data including speed, location, fines.
- **Kafka Consumer:** Consumes events in batches and writes partitioned CSVs to MinIO.
- **Spark Structured Streaming:** Reads raw data from MinIO, parses timestamps, partitions data by year/month/day and writes to processed zone.
- **Snowflake Loader:** Incrementally loads processed Parquet data from MinIO to Snowflake using a MERGE strategy (upserts).
- **Airflow DAG:** (Recommended) Orchestrates Snowflake loads on an hourly schedule.
---

##  **Tech Stack**

- **Python**: Data generation, Kafka producer/consumer, Snowflake loader
- **Apache Kafka**: Event streaming backbone
- **MinIO (S3)**: Data lake storage for raw & processed data
- **Apache Spark Structured Streaming**: ETL, window aggregations
- **Snowflake**: Cloud data warehouse
- **Apache Airflow:** Workflow orchestration for automation

---
## How it works

**Producer:** `vehicle_producer.py` continuously streams random vehicle events to Kafka.  
**Consumer:** `batch_consumer.py` reads the Kafka topic, buffers messages, and flushes CSV files to MinIO every few seconds or when the batch size is reached.  
**Spark Processor:** `streaming_job.py` reads raw CSVs from MinIO, adds `event_time`, `year`, `month`, and `day` columns, and writes clean Parquet data back to MinIO.  
**Snowflake Loader:** `snowflake_loader.py` scans processed files in MinIO and merges data into Snowflake using `MERGE` for upserts.  
**Airflow DAG:** `snowflake_loader_dag.py` runs the loader hourly to keep Snowflake fresh.

---

##  **Key Learning**

- Real-time streaming using Kafka + Spark
- Partitioning and event-time handling in Spark Structured Streaming
- Working with MinIO as S3-compatible storage
- Batch ingestion and MERGE into Snowflake
- Orchestrating data pipelines with Airflow

---

##  **Run it**

- `pip install -r requirements.txt`
- Run Kafka + MinIO (or use `docker-compose.yml`)
- Start the producer: `python producer/vehicle_producer.py`
- Start the consumer: `python consumer/batch_consumer.py`
- Start Spark job:
-                   docker exec -it stockmarketdatapipeline-spark-master-1 bash
-                   spark-submit \
                    --master spark://spark-master:7077 \
                    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
                     /opt/spark/jobs/vehicle-info_PySpark_Streaming.py
- Load to Snowflake: `python snowflake_loader/snowflake_loader.py`
- Airflow run it hourly: snowflake_loader_dag.py in your dags/ folder and run Airflow

---

##  **Author**

Built by [HANUMANT NANASAHEB THAKARE] — Data Engineering Portfolio 

---


