from netflix_pipeline.bronze import add_bronze_metadata

def test_add_bronze_metadata(spark):
    # 1. Arrange: Create a tiny fake dataframe
    data = [{"movieId": 1, "title": "Toy Story (1995)"}]
    df = spark.createDataFrame(data)
    
    # 2. Act: Run our transformation logic
    transformed_df = add_bronze_metadata(df)
    
    # 3. Assert: Check if the new columns exist
    columns = transformed_df.columns
    
    assert "ingest_timestamp" in columns, "ingest_timestamp column is missing!"
    assert "source_file" in columns, "source_file column is missing!"
    
    # Ensure our original data wasn't deleted
    assert "movieId" in columns
    assert transformed_df.count() == 1