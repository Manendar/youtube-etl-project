from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, desc, year, month, to_date
from spark_jobs.utils.spark_helpers import get_spark_session, save_to_hdfs
from scripts.config_loader import load_config
import sys

def write_to_postgres(df, table_name, db_url, db_properties):
    """Write a Spark dataframe to a PostgreSQL table"""
    print(f"Writing {table_name} to PostgreSQL...", flush=True)
    df.write \
        .jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)
    print(f"✓ {table_name} written to PostgreSQL", flush=True)

def process_youtube_data():
    print("Starting Spark job...", flush=True)

    config = load_config()
    staging_path = f"hdfs://namenode:9000{config['hdfs_base_path']}/{config['staging_zone']}/"
    processed_path = f"hdfs://namenode:9000{config['hdfs_base_path']}/{config['processed_zone']}/"

    # PostgreSQL connection settings
    db_url = f"jdbc:postgresql://postgres:5432/youtube_analytics"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    try:
        spark = get_spark_session("YouTube Data Processing")
        print("Spark session created successfully", flush=True)
    except Exception as e:
        print(f"Failed to create Spark session: {e}", flush=True)
        sys.exit(1)

    try:
        print("Reading data from staging zone...", flush=True)
        df = spark.read.option("multiLine", "true").option("quote", '"').option("escape", '"').csv(staging_path + "*.csv", header=True, inferSchema=True)
        record_count = df.count()
        print(f"Total records loaded: {record_count}", flush=True)
    except Exception as e:
        print(f"Error reading data from staging zone: {e}", flush=True)
        spark.stop()
        sys.exit(1)

    try:
        print("Processing top videos by views...", flush=True)
        top_videos = df.select(
            "video_id", "title", "channel_title", "views", "likes", "dislikes", "comment_count"
        ).orderBy(desc("views")).limit(1000)
        save_to_hdfs(top_videos, f"{processed_path}top_videos_by_views", "parquet")
        write_to_postgres(top_videos, "top_videos_by_views", db_url, db_properties)
    except Exception as e:
        print(f"Error processing top videos: {e}", flush=True)

    try:
        print("Processing channel statistics...", flush=True)
        channel_stats = df.groupBy("channel_title").agg(
            count("video_id").alias("total_videos"),
            avg("views").alias("avg_views"),
            spark_sum("views").alias("total_views"),
            avg("likes").alias("avg_likes"),
            avg("comment_count").alias("avg_comments")
        ).orderBy(desc("total_views"))
        save_to_hdfs(channel_stats, f"{processed_path}channel_statistics", "parquet")
        write_to_postgres(channel_stats, "channel_statistics", db_url, db_properties)
    except Exception as e:
        print(f"Error processing channel statistics: {e}", flush=True)

    try:
        print("Processing category performance...", flush=True)
        category_stats = df.groupBy("category_id").agg(
            count("video_id").alias("video_count"),
            avg("views").alias("avg_views"),
            avg("likes").alias("avg_likes"),
            avg("dislikes").alias("avg_dislikes")
        ).orderBy(desc("avg_views"))
        save_to_hdfs(category_stats, f"{processed_path}category_performance", "parquet")
        write_to_postgres(category_stats, "category_performance", db_url, db_properties)
    except Exception as e:
        print(f"Error processing category performance: {e}", flush=True)

    try:
        print("Processing trending patterns...", flush=True)
        df_with_date = df.withColumn("trending_date_parsed", to_date(col("trending_date"))) \
                         .withColumn("year", year(col("trending_date_parsed"))) \
                         .withColumn("month", month(col("trending_date_parsed")))
        trending_patterns = df_with_date.groupBy("year", "month").agg(
            count("video_id").alias("trending_count"),
            avg("views").alias("avg_views")
        ).orderBy("year", "month")
        save_to_hdfs(trending_patterns, f"{processed_path}trending_patterns", "parquet")
        write_to_postgres(trending_patterns, "trending_patterns", db_url, db_properties)
    except Exception as e:
        print(f"Error processing trending patterns: {e}", flush=True)

    try:
        print("Processing high engagement videos...", flush=True)
        engagement = df.withColumn(
            "engagement_rate",
            (col("likes") + col("dislikes") + col("comment_count")) / col("views") * 100
        ).select(
            "video_id", "title", "channel_title", "views", "engagement_rate"
        ).orderBy(desc("engagement_rate")).limit(1000)
        save_to_hdfs(engagement, f"{processed_path}high_engagement_videos", "parquet")
        write_to_postgres(engagement, "high_engagement_videos", db_url, db_properties)
    except Exception as e:
        print(f"Error processing high engagement videos: {e}", flush=True)

    print("All analytics completed!", flush=True)
    spark.stop()

if __name__ == '__main__':
    process_youtube_data()
