from pyspark.sql import SparkSession
from netflix_pipeline.bronze import process_bronze_layer
from netflix_pipeline.silver import process_silver_layer_wap
from netflix_pipeline.gold import process_gold_layer

def get_prod_spark_session():
    # This configures Spark to use the actual AWS Glue Catalog and S3
    return SparkSession.builder \
        .appName("Netflix-Production-Pipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

if __name__ == "__main__":
    print("🚀 STARTING NETFLIX PRODUCTION PIPELINE...")
    spark = get_prod_spark_session()

    # 1. Create the database in AWS Glue if it doesn't exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.netflix_db")

    # 2. Define the S3 bucket (Replace with your actual bucket name suffix!)
    bucket = "s3://sumanth-de-bucket-p1"

    # 3. Execute the Medallion Architecture
    print("\n--- RUNNING BRONZE LAYER ---")
    process_bronze_layer(spark, f"{bucket}/raw/movielens/movies.csv", "bronze_movies")
    process_bronze_layer(spark, f"{bucket}/raw/movielens/ratings.csv", "bronze_ratings")

    print("\n--- RUNNING SILVER LAYER (WAP) ---")
    process_silver_layer_wap(spark)

    print("\n--- RUNNING GOLD LAYER ---")
    process_gold_layer(spark)

    print("\n✅ PIPELINE COMPLETE!")
    spark.stop()venv-pack -o environment.tar.gz