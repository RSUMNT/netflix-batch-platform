from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from netflix_pipeline.gold import generate_movie_stats, generate_user_stats

def test_gold_layer_aggregations(spark):
    # 1. Arrange: Create mock "Silver" data (Clean, no nulls, casted types)
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", DoubleType(), True)
    ])
    
    # Scenario: 
    # User 1 rates Movie 100 a 4.0 and Movie 101 a 5.0 (Avg: 4.5, Count: 2)
    # User 2 rates Movie 100 a 3.0 (Avg: 3.0, Count: 1)
    # Movie 100 should have Avg: 3.5, Count: 2
    mock_silver_data = [
        (1, 100, 4.0),
        (1, 101, 5.0),
        (2, 100, 3.0)
    ]
    
    df = spark.createDataFrame(mock_silver_data, schema=schema)
    
    # 2. Act: Run our Gold aggregation logic
    movie_stats_df = generate_movie_stats(df)
    user_stats_df = generate_user_stats(df)
    
    # Convert results to Python dictionaries so they are easy to query in our assertions
    movie_results = {row["movieId"]: row for row in movie_stats_df.collect()}
    user_results = {row["userId"]: row for row in user_stats_df.collect()}
    
    # 3. Assert: Verify Movie Popularity Stats
    assert movie_results[100]["total_ratings"] == 2, "Movie 100 count failed"
    assert movie_results[100]["avg_rating"] == 3.5, "Movie 100 average failed"
    
    assert movie_results[101]["total_ratings"] == 1, "Movie 101 count failed"
    assert movie_results[101]["avg_rating"] == 5.0, "Movie 101 average failed"
    
    # 4. Assert: Verify User Activity Stats
    assert user_results[1]["movies_rated"] == 2, "User 1 count failed"
    assert user_results[1]["avg_rating_given"] == 4.5, "User 1 average failed"
    
    assert user_results[2]["movies_rated"] == 1, "User 2 count failed"
    assert user_results[2]["avg_rating_given"] == 3.0, "User 2 average failed"