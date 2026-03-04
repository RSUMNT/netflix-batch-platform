from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from netflix_pipeline.utils.data_quality import run_silver_quality_checks

# --- TRANSFORMATION LOGIC ---
def clean_ratings_data(df):
    casted_df = df.withColumn("userId", col("userId").cast("integer")) \
                  .withColumn("movieId", col("movieId").cast("integer")) \
                  .withColumn("rating", col("rating").cast("double"))
    no_nulls_df = casted_df.dropna(subset=["userId", "movieId", "rating"])
    dedup_df = no_nulls_df.dropDuplicates(["userId", "movieId", "timestamp"])
    return dedup_df

def get_spark_session():
    return SparkSession.builder \
        .appName("Netflix-Silver-Layer-WAP") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()

# --- THE WRITE-AUDIT-PUBLISH PATTERN ---
def process_silver_layer_wap(spark):
    table_name = "glue_catalog.netflix_db.silver_ratings"
    branch_name = "audit_staging"
    
    print("1. Reading from Bronze Ratings...")
    bronze_df = spark.table("glue_catalog.netflix_db.bronze_ratings")
    silver_df = clean_ratings_data(bronze_df)
    
    # Ensure the base table exists before we try to branch it
    # (Iceberg quirk: you can't branch a table that doesn't exist yet)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            userId INT, movieId INT, rating DOUBLE, timestamp STRING
        ) USING iceberg
    """)

    # Create our hidden staging branch
    print(f"2. Creating hidden Iceberg branch: '{branch_name}'...")
    spark.sql(f"ALTER TABLE {table_name} CREATE BRANCH IF NOT EXISTS {branch_name}")
    
    # WRITE Phase
    print("3. WRITE: Appending physical data to the hidden branch...")
    silver_df.write \
        .format("iceberg") \
        .option("branch", branch_name) \
        .mode("append") \
        .insertInto(table_name)
        
    # AUDIT Phase
    print("4. AUDIT: Reading physical data from the branch and running Great Expectations...")
    # We read exactly what was just written to disk to prevent false positives
    audit_df = spark.read \
        .format("iceberg") \
        .option("branch", branch_name) \
        .table(table_name)
        
    try:
        run_silver_quality_checks(audit_df)
    except ValueError as e:
        print(f"🚨 AUDIT FAILED! The 'main' branch remains untouched. Error: {e}")
        # In production, we could drop the bad branch here to clean up storage
        raise e

    # PUBLISH Phase
    print("5. PUBLISH: Audit passed! Fast-forwarding 'main' branch to match staging...")
    spark.sql(f"CALL glue_catalog.system.fast_forward('{table_name}', 'main', '{branch_name}')")
    
    print("✅ WAP Cycle Complete! Production data is successfully updated.")

if __name__ == "__main__":
    spark = get_spark_session()
    process_silver_layer_wap(spark)
    spark.stop()