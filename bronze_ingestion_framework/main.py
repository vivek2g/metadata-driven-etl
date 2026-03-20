import sys 
from pyspark.sql import SparkSession
from config.db_connector import get_db_connection, fetch_schema_mapping
from readers.source_reader import read_source_data

def main():
    print("=== STARTING AWS SMOKE TEST ===")
    
    # 1. Initialize Spark
    spark = SparkSession.builder.appName("Module_1_Test").getOrCreate()
    
    # 2. Test Postgres Connection
    try:
        print("Attempting to connect to AWS Postgres...")
        db_conn = get_db_connection()
        metadata = fetch_schema_mapping(db_conn, dataset_id=2)
        print(f"SUCCESS! Fetched {len(metadata)} columns from Postgres control_plane.")
        print(f"Sample Metadata: {metadata[0]}")
    except Exception as e:
        print(f"FAILED to connect to Postgres: {e}")
        sys.exit(1)
        
    # 3. Test S3 Access & Reader
    try:
        # REPLACE THIS with an actual path to a CSV in your S3 bucket
        s3_test_path = "s3://your-landing-bucket/sales/test_file.csv" 
        print(f"Attempting to read from {s3_test_path}...")
        
        test_df = read_source_data(spark, s3_test_path)
        print("SUCCESS! File read. Here is the schema:")
        test_df.printSchema()
        
    except Exception as e:
        print(f"FAILED to read from S3: {e}")
        sys.exit(1)

    print("=== SMOKE TEST COMPLETE ===")

if __name__ == "__main__":
    main()