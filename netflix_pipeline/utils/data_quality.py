import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def run_silver_quality_checks(spark_df):
    """
    Runs Great Expectations data quality checks on the Silver DataFrame.
    If any check fails, it raises an exception to halt the pipeline.
    """
    print("Initializing Great Expectations Audit...")
    
    # Wrap the PySpark DataFrame in Great Expectations
    ge_df = SparkDFDataset(spark_df)
    
    validation_results = []
    
    # 1. Column Value Check: Ratings must be between 0.5 and 5.0
    print("Running Check: Ratings between 0.5 and 5.0...")
    res1 = ge_df.expect_column_values_to_be_between(
        column="rating", 
        min_value=0.5, 
        max_value=5.0
    )
    validation_results.append(res1)
    
    # 2. Compound Uniqueness Check: No duplicate ratings per user-movie combo
    print("Running Check: Unique user-movie combinations...")
    res2 = ge_df.expect_compound_columns_to_be_unique(
        column_list=["userId", "movieId"]
    )
    validation_results.append(res2)
    
    # 3. Row Count Check: Ensure we didn't accidentally drop all our data
    # (Since we might test locally with tiny data, we set min to 1. 
    # In prod, you'd set this to ~20,000,000 for the MovieLens dataset)
    print("Running Check: Table row count within expected bounds...")
    res3 = ge_df.expect_table_row_count_to_be_between(
        min_value=1, 
        max_value=30000000
    )
    validation_results.append(res3)
    
    # Evaluate the results
    failed_checks = [res for res in validation_results if not res["success"]]
    
    if failed_checks:
        # In a real Netflix environment, this triggers a Slack/PagerDuty alert
        print(f"FAILED CHECKS: {failed_checks}")
        raise ValueError("Data Quality Checks Failed! Halting pipeline to prevent data corruption.")
    
    print("✅ All Data Quality Checks Passed! Data is safe to publish.")
    return True