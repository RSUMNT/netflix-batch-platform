from netflix_pipeline.silver import clean_ratings_data
from pyspark.sql.types import StructType, StructField, StringType

def test_clean_ratings_data(spark):
    # 1. Arrange: Create a dirty dataframe
    # We use strings here to test type casting, and include a null userId and a duplicate
    schema = StructType([
        StructField("userId", StringType(), True),
        StructField("movieId", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    dirty_data = [
        ("1", "100", "4.5", "1630000000"), # Good row
        (None, "101", "3.0", "1630000001"), # Bad row: Null userId
        ("1", "100", "4.5", "1630000000"), # Bad row: Exact duplicate of row 1
    ]
    
    df = spark.createDataFrame(dirty_data, schema=schema)
    
    # 2. Act: Run our cleaning logic
    cleaned_df = clean_ratings_data(df)
    
    # 3. Assert: Verify the cleaning worked
    results = cleaned_df.collect()
    
    # We should only have 1 row left (dropped the null, dropped the duplicate)
    assert len(results) == 1, f"Expected 1 row, got {len(results)}"
    
    # Check that types were casted correctly (rating should be float/double, not string)
    assert isinstance(results[0]["rating"], float), "Rating was not cast to double/float"
    assert isinstance(results[0]["userId"], int), "UserId was not cast to int"