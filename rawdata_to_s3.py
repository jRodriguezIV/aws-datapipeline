import os
import logging
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, date_format, lit, current_timestamp, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType

# ---------- CONFIG ----------
load_dotenv()

LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR")
LOG_DIR = os.getenv("LOG_DIR")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX_ROOT = os.getenv("S3_PREFIX_ROOT")
s3 = boto3.client("s3")

DRY_RUN = False # <--- set to False actually write to S3

logging.basicConfig(
    filename=f'{LOG_DIR}/rawdata_to_s3_etl.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

schema = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("Close", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Open", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Date", DateType(), True),
    StructField("TimeZone", StringType(), True),
    StructField("Time", StringType(), True)
])

rename_map = {
    "Datetime": "datetime",
    "Close": "closing_price",
    "High": "high_price",
    "Low": "low_price",
    "Open": "open_price",
    "Volume": "volume"
}

# ---------- INIT SPARK ----------
spark = SparkSession.builder \
    .appName("Dynamic_RawData_ETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
# Reduce Spark log verbosity
spark.sparkContext.setLogLevel("WARN")
window_spec = Window.partitionBy("date").orderBy("datetime").rowsBetween(-4, 0)

# ---------- DYNAMIC ETL ----------
for file_name in os.listdir(LOCAL_DATA_DIR):
    existing_dates = []
    if file_name.startswith("archived_") and file_name.endswith(".csv"):
        try:
            file_path = os.path.join(LOCAL_DATA_DIR, file_name)
            symbol = file_name.replace("archived_", "").replace("_results.csv", "")

            # Check existing dates in S3 directory
            prefix = f"{S3_PREFIX_ROOT}/{symbol}/"
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/')
            if "CommonPrefixes" in response:
                existing_dates = [p['Prefix'].split('/')[-2] for p in response['CommonPrefixes']]

            # Extract: Load CSV
            df = spark.read.csv(file_path, header=True, schema=schema)

            # Transform: Drop redundant column
            cols_to_drop = [c for c in ["Date", "Time", "TimeZone"] if c in df.columns]
            df = df.drop(*cols_to_drop)
            df_clean = df.dropna()
            df_clean = df_clean.withColumn("date", to_date(col("Datetime")))
            df_clean = df_clean.filter(~col("date").cast("string").isin(existing_dates))

            # Transform: Rename Columns
            for old, new in rename_map.items():
                if old in df_clean.columns:
                    df_clean = df_clean.withColumnRenamed(old, new)

            # Transform: Add Metadata / Audit Columns, Moving Average 5 transformations
            df_clean = (
                df_clean 
                .withColumn("time", date_format(col("datetime"), "HH:mm:ss"))  
                .withColumn("source_file", lit(file_name)) 
                .withColumn("ingested_at", current_timestamp()) 
                .withColumn("MA5", avg("closing_price").over(window_spec))
                )           
        
            # Optional: add more transformations here (moving averages, indicators, etc.)\

            # Load: Write to S3 (Parquet)
            output_path = f"s3a://{S3_BUCKET}/{S3_PREFIX_ROOT}/{symbol}"
            if DRY_RUN:
                print(f"[DRY-RUN]: ETL complete for {file_name} → {output_path}")
                print(f"Sample rows:\n", df_clean.show(5))
            else : 
                dates = [row['date'] for row in df_clean.select("date").distinct().collect()]

                for d in dates:
                    date_filtered_df = df_clean.filter(col("date") == d)
                    output_path_date = f"{output_path}/{d}"
                    date_filtered_df.write.mode("overwrite").parquet(output_path_date)
                    
                    

                logging.info(f"ETL complete for {file_name} → {output_path}")
        except Exception as e:
            logging.error(f"Failed ETL for {file_name}: {e}", exc_info=True)

# ---------- STOP SPARK ----------
spark.stop()