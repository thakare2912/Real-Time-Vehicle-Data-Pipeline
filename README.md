# Real-Time Vehicle Telematics Data Pipeline

This project simulates, ingests, processes, and analyzes **real-time vehicle telematics data** using modern Data Engineering tools:
- **Kafka** for streaming data
- **MinIO (S3)** for raw & processed storage
- **Spark Structured Streaming** for near real-time processing
- **Snowflake** for warehousing & analytics
- **Docker** for local orchestration

---

## Features

Simulates random vehicle events (speed, location, fines)  
Streams vehicle events to Kafka  
Consumes events in batches & stores as CSV in MinIO  
Runs a Spark Structured Streaming job to read raw CSV batches, enrich & transform to Parquet  
Loads processed data incrementally into Snowflake using an upsert (MERGE)  
Fully containerized with Docker

---
---

##  **Run it**

- `pip install -r requirements.txt`
- Run Kafka + MinIO (or use `docker-compose.yml`)
- Start the producer: ` python producer/vehicle_producer.py `
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


