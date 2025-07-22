

import logging
import sys
import traceback
from datetime import datetime

import boto3
import pandas as pd
import numpy as np
import snowflake.connector
import pyarrow.parquet as pq
import io


# Logging Config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)




# MinIO
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "vehicle-data"

# Snowflake
SNOWFLAKE_ACCOUNT = ""
SNOWFLAKE_USER =  ""
SNOWFLAKE_PASSWORD = ''
SNOWFLAKE_DATABASE = "VEHICLE"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "VEHICLE_data"



def init_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

def init_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


#  Table Setup


def create_snowflake_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        vehicle_id STRING,
        number_plate STRING,
        vehicle_type STRING,
        vehicle_name STRING,
        speed FLOAT,
        location_lat FLOAT,
        location_lon FLOAT,
        user_id INT,
        phone_number STRING,
        car_life_years INT,
        price_of_vechical FLOAT,
        fine INT,
        timestamp FLOAT,
        event_time TIMESTAMP,
        year INT,
        month INT,
        day INT,
        last_updated TIMESTAMP,
        PRIMARY KEY (vehicle_id, event_time)
    )
    """
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_query)
        logger.info(f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} table created")
    finally:
        cursor.close()


# Read Parquet from MinIO

def read_processed_parquet(s3_client):
    prefix = "processed/vehicle/"
    logger.info(f"Scanning: s3://{S3_BUCKET}/{prefix}")

    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in response:
        logger.info("No files found.")
        return None

    dfs = []

    for obj in response["Contents"]:
        if not obj["Key"].endswith(".parquet"):
            continue

        logger.info(f"Reading file: {obj['Key']}")
        response_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
        parquet_data = response_obj["Body"].read()
        parquet_buffer = io.BytesIO(parquet_data)
        table = pq.read_table(parquet_buffer)
        df = table.to_pandas()

        
        key_parts = obj["Key"].split("/")
        partitions = {}
        for part in key_parts:
            if "=" in part:
                k, v = part.split("=")
                partitions[k] = int(v)

        # Add partition columns manually
        for col in ['year', 'month', 'day']:
            df[col] = partitions.get(col, None)

        df['last_updated'] = datetime.now()

        dfs.append(df)

    if not dfs:
        logger.info("No valid parquet files found.")
        return None

    df_all = pd.concat(dfs, ignore_index=True)

    # Fix epoch -> datetime
    for col in ["processing_time", "event_time"]:
        if col in df_all.columns:
            if pd.api.types.is_numeric_dtype(df_all[col]):
                df_all[col] = pd.to_datetime(df_all[col], unit='s', errors='coerce')
            else:
                df_all[col] = pd.to_datetime(df_all[col], errors='coerce')

    logger.info(f"Loaded {len(df_all)} rows.")
    return df_all


#  MERGE Load to Snowflake

def incremental_load_to_snowflake(conn, df):
    if df is None or df.empty:
        logger.info("No data to load.")
        return

    required_columns = {
        'vehicle_id', 'number_plate', 'vehicle_type', 'vehicle_name',
        'speed', 'location_lat', 'location_lon', 'user_id',
        'phone_number', 'car_life_years', 'price_of_vechical',
        'fine',  'timestamp', 'event_time',
        'year', 'month', 'day', 'last_updated'
    }

    # Fix CSV column name typo
    if 'price_of_vechical' in df.columns:
        df = df.rename(columns={'price_of_vechical': 'price_of_vechical'})

    if 'price_of_vehicle' not in df.columns:
        logger.error("Missing column: price_of_vechical")
        return
def incremental_load_to_snowflake(conn, df):
    """Incrementally load processed vehicle data to Snowflake."""
    if df is None or df.empty:
        logger.info("No data to load.")
        return

    valid_df = df.copy()

    cursor = conn.cursor()
    try:
        # 1) Create temporary staging table
        stage_table = "TEMP_STAGE_VEHICLE"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")

        #  2) Prepare INSERT statement
        records = valid_df.to_records(index=False)
        columns = list(valid_df.columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {stage_table} ({','.join(columns)}) VALUES ({placeholders})"

        # 3) Convert rows with timestamp handling
        record_list = []
        for record in records:
            record_tuple = tuple(
                None if pd.isna(val)
                else val.isoformat() if isinstance(val, pd.Timestamp)
                else val.isoformat() if isinstance(val, datetime)
                else str(val) if isinstance(val, str)
                else float(val) if isinstance(val, (np.floating,))
                else int(val) if isinstance(val, (np.integer,))
                else val
                for val in record
            )
            record_list.append(record_tuple)

        logger.info(f"Inserting {len(record_list)} rows into staging...")
        cursor.executemany(insert_query, record_list)

        #  4) MERGE from staging into target table
        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.vehicle_id = source.vehicle_id AND target.event_time = source.event_time
        WHEN MATCHED THEN UPDATE SET
            number_plate = source.number_plate,
            vehicle_type = source.vehicle_type,
            vehicle_name = source.vehicle_name,
            speed = source.speed,
            location_lat = source.location_lat,
            location_lon = source.location_lon,
            user_id = source.user_id,
            phone_number = source.phone_number,
            car_life_years = source.car_life_years,
            price_of_vechical = source.price_of_vechical,
            fine = source.fine,
            timestamp = source.timestamp,
            year = source.year,
            month = source.month,
            day = source.day,
            last_updated = source.last_updated
        WHEN NOT MATCHED THEN INSERT (
            vehicle_id, number_plate, vehicle_type, vehicle_name,
            speed, location_lat, location_lon, user_id, phone_number,
            car_life_years, price_of_vechical, fine, timestamp,
             event_time, year, month, day, last_updated
        ) VALUES (
            source.vehicle_id, source.number_plate, source.vehicle_type, source.vehicle_name,
            source.speed, source.location_lat, source.location_lon, source.user_id,
            source.phone_number, source.car_life_years, source.price_of_vechical,
            source.fine, source.timestamp, source.event_time,
            source.year, source.month, source.day, source.last_updated
        )
        """
        cursor.execute(merge_query)
        logger.info("MERGE complete.")

    except Exception as e:
        logger.error(f"Error during incremental load: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        cursor.close()


# Main

def main():
    logger.info(" Starting Vehicle Data Loader")

    s3_client = init_s3_client()
    conn = init_snowflake_connection()

    try:
        create_snowflake_table(conn)
        df = read_processed_parquet(s3_client)
        incremental_load_to_snowflake(conn, df)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
    finally:
        conn.close()
        logger.info("Job Done.")

if __name__ == "__main__":
    main()
