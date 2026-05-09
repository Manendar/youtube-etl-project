from pyspark.sql import SparkSession
import sys

def get_spark_session(app_name="YouTube ETL"):
    print(f"Creating Spark session: {app_name}...", flush=True)
    sys.stdout.flush()
    
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("Spark session created successfully", flush=True)
        sys.stdout.flush()
        return spark
    except Exception as e:
        print(f"Failed to create Spark session: {e}", flush=True)
        sys.stdout.flush()
        raise

def save_to_hdfs(dataframe, path, format="parquet"):
    print(f"Saving data to {path}...", flush=True)
    sys.stdout.flush()
    try:
        dataframe.write.mode("overwrite").format(format).save(path)
        print(f"Data saved successfully to {path}", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"Error saving data to {path}: {e}", flush=True)
        sys.stdout.flush()
        raise

def read_from_hdfs(spark, path, format="parquet"):
    print(f"Reading data from {path}...", flush=True)
    sys.stdout.flush()
    try:
        df = spark.read.format(format).load(path)
        print(f"Data loaded successfully from {path}", flush=True)
        sys.stdout.flush()
        return df
    except Exception as e:
        print(f"Error reading data from {path}: {e}", flush=True)
        sys.stdout.flush()
        raise
