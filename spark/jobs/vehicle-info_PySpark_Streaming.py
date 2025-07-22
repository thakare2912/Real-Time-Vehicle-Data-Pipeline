

import logging
import os
import sys
import traceback
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType
)


# Logging Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# Configs


MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "vehicle-data"
MINIO_ENDPOINT = "http://minio:9000"

# Spark Session


def create_spark_session():
    """Create and configure a Spark session for streaming."""
    logger.info("Initializing Spark session with S3 configuration for streaming...")
    
    spark = (SparkSession.builder
        .appName("VehicleStreamingProcessor")
        .config("spark.jars.packages", 
               "org.apache.hadoop:hadoop-aws:3.3.4,"
               "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.metrics.logger.level", "WARN")  # Suppress metrics warning
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())
    
    # Configure S3A for MinIO
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized.")
    return spark


# Schema (Updated to match CSV)


def define_vehicle_schema():
    return StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("number_plate", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("vehicle_name", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("location_lat", DoubleType(), True),
        StructField("location_lon", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("phone_number", StringType(), True),
        StructField("car_life_years", IntegerType(), True),
        StructField("price_of_vechical", DoubleType(), True),  # Matches CSV header
        StructField("fine", IntegerType(), True),
        StructField("timestamp", DoubleType(), True)
    ])


# Read from MinIO (Updated with date partitioning)


def read_stream_from_minio_raw(spark):
    """Read streaming vehicle data from MinIO with date partitioning."""
    logger.info("Setting up MinIO streaming source...")

    today = datetime.now()
    s3_path = f"s3a://{MINIO_BUCKET}/raw/vehicle/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
    logger.info(f"Reading streaming data from: {s3_path}")

    try:
        df_stream = (
            spark.readStream
            .option("header", "true")
            .option("maxFilesPerTrigger", 10)  # Control processing rate
            .schema(define_vehicle_schema())
            .csv(s3_path)
        )
        
        

        # Add processing timestamp and date partitions
        df_stream = (
            df_stream
            .withColumn("event_time", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
            .withColumn("year", F.year(F.col("event_time")))
            .withColumn("month", F.month(F.col("event_time")))
            .withColumn("day", F.dayofmonth(F.col("event_time")))
            
        )
        
        return df_stream
    except Exception as e:
        logger.error(f"Failed to read from MinIO: {str(e)}")
        raise


# Write batch to MinIO (Updated to write to processed path)


def process_and_write_batch(df, batch_id):
    if df.count() == 0:
        logger.info(f"Batch {batch_id} has no rows.")
        return

    logger.info(f"Processing batch {batch_id} with {df.count()} rows")
    
    # Show sample data
    df.select("vehicle_id", "number_plate", "speed", "event_time").limit(5).show()
    
    # Write to processed location
    output_path = f"s3a://{MINIO_BUCKET}/processed/vehicle/"
    logger.info(f"Writing batch {batch_id} to {output_path}")

    (df.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(output_path))

    logger.info(f"Batch {batch_id} written successfully")


# Main Function
def main():
    logger.info("\n=============================")
    logger.info("STARTING VEHICLE STREAMING JOB")
    logger.info("=============================")

    spark = create_spark_session()

    try:
        df = read_stream_from_minio_raw(spark)
        checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/vehicle-processor"

        query = (df.writeStream
            .foreachBatch(process_and_write_batch)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .trigger(processingTime="1 minute")
            .start())

        logger.info("Vehicle streaming processor running...")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Streaming job failed: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()
        logger.info("Streaming processor completed.")

if __name__ == "__main__":
    main()