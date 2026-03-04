from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round

# --- TRANSFORMATION LOGIC (Testable) ---
def generate_movie_stats(df):
    """Aggregates movie popularity metrics for ML features."""
    return df.groupBy("movieId") \
             .agg(
                 count("rating").alias("total_ratings"),
                 round(avg("rating"), 2).alias("avg_rating")
             )

def generate_user_stats(df):
    """Aggregates user activity metrics for ML features."""
    return df.groupBy("userId") \
             .agg(
                 count("rating").alias("movies_rated"),
                 round(avg("rating"), 2).alias("avg_rating_given")
             )
# ---------------------------------------

def get_spark_session():
    return SparkSession.builder \
        .appName("Netflix-Gold-Layer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()

def process_gold_layer(spark):
    print("Reading from Silver Ratings table...")
    # Read the clean data
    silver_df = spark.table("glue_catalog.netflix_db.silver_ratings")
    
    # Generate the Gold DataFrames
    print("Aggregating ML Features...")
    movie_stats_df = generate_movie_stats(silver_df)
    user_stats_df = generate_user_stats(silver_df)
    
    # Write Movie Stats to Iceberg
    print("Writing gold_movie_stats...")
    movie_stats_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("glue_catalog.netflix_db.gold_movie_stats")

    # Write User Stats to Iceberg
    print("Writing gold_user_stats...")
    user_stats_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("glue_catalog.netflix_db.gold_user_stats")
        
    print("Successfully processed Gold layer!")

if __name__ == "__main__":
    spark = get_spark_session()
    process_gold_layer(spark)
    spark.stop()