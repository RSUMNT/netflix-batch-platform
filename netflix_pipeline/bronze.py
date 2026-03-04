import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# --- THIS IS THE FUNCTION WE WILL TEST ---
def add_bronze_metadata(df):
    """Adds data lineage metadata to raw dataframes."""
    return df \
        .withColumn("ingest_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
# -----------------------------------------

def get_spark_session():
    return SparkSession.builder \
        .appName("Netflix-Bronze-Layer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://sumanth-de-bucket-p1/warehouse/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

def process_bronze_layer(spark, source_path, table_name):
    print(f"Reading raw data from {source_path}...")
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
    
    # Use our new modular function
    bronze_df = add_bronze_metadata(raw_df)
    
    print(f"Writing to Bronze Iceberg table: {table_name}...")
    bronze_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(f"glue_catalog.netflix_db.{table_name}")
    print(f"Successfully processed {table_name}")

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.netflix_db")
    
    bucket = "s3://sumanth-de-bucket-p1"
    process_bronze_layer(spark, f"{bucket}/raw/movielens/movies.csv", "bronze_movies")
    process_bronze_layer(spark, f"{bucket}/raw/movielens/ratings.csv", "bronze_ratings")
    spark.stop()