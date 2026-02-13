# Initial Pipeline Trigger - v1.0
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. GET PARAMETERS
# We only need ONE argument: The Table Name. The rest comes from the DB.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TABLE_NAME', 'METADATA_CONN_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

table_name_param = args['TABLE_NAME']
db_connection_name = args['METADATA_CONN_NAME'] # e.g., "Postgres_Control_Plane"

print(f"--- Starting Job for Table: {table_name_param} ---")

# -------------------------------------------------------------------------
# 2. FETCH METADATA (The "Brain")
# -------------------------------------------------------------------------
# We use Spark JDBC to read the config directly from Postgres
# This is better than 'pg8000' because it uses built-in Glue drivers.

# A. Get Table Details
table_query = f"""
    SELECT table_id, source_s3_path, file_format, delimiter, has_header, 
           watermark_col_name, last_watermark_value, load_type
    FROM control_plane.bronze_table_details 
    WHERE table_name = '{table_name_param}' AND is_active = true
"""

# Read from JDBC (using Glue Connection properties)
# Note: In a real job, you might fetch credentials from Secrets Manager. 
# For now, we assume the Glue Connection has them.
df_table_config = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": f"({table_query}) as t",
        "connectionName": db_connection_name
    }
).toDF()

if df_table_config.count() == 0:
    raise Exception(f"No active configuration found for table: {table_name_param}")

# Collect the config into a Python dictionary/Row
config = df_table_config.collect()[0]
table_id = config['table_id']
source_path = config['source_s3_path']
file_fmt = config['file_format']
watermark_col = config['watermark_col_name']
last_watermark = config['last_watermark_value']

print(f"--- Configuration Loaded. Source: {source_path} | Format: {file_fmt} ---")

# B. Get Column Details (The "Contract")
col_query = f"""
    SELECT source_col_name, target_col_name, data_type, is_required, is_pii 
    FROM control_plane.bronze_column_details 
    WHERE table_id = {table_id}
"""

df_col_config = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": f"({col_query}) as c",
        "connectionName": db_connection_name
    }
).toDF().collect()

# Convert list of Rows to a dictionary for easy lookup
# Format: {'EmpID': {'target': 'employee_id', 'is_pii': False, 'required': True}}
column_mapping = {
    row['source_col_name']: {
        'target': row['target_col_name'], 
        'is_pii': row['is_pii'],
        'required': row['is_required']
    } for row in df_col_config
}

# -------------------------------------------------------------------------
# 3. READ SOURCE DATA
# -------------------------------------------------------------------------
# Dynamic reader based on format
options = {}
if file_fmt == 'csv':
    options = {"header": "true" if config['has_header'] else "false", "delimiter": config['delimiter']}

df_source = spark.read.format(file_fmt).options(**options).load(source_path)

# -------------------------------------------------------------------------
# 4. APPLY SCHEMA & VALIDATION
# -------------------------------------------------------------------------

# A. Check for REQUIRED columns
source_columns = df_source.columns
for src_col, rules in column_mapping.items():
    if rules['required'] and src_col not in source_columns:
        raise Exception(f"SCHEMA ERROR: Missing required column '{src_col}' in source file.")

# B. Incremental Filter (Watermark)
# We only filter if we have a valid watermark column and value
if watermark_col and last_watermark and watermark_col in source_columns:
    print(f"--- Filtering data > {last_watermark} ---")
    df_source = df_source.filter(F.col(watermark_col) > last_watermark)

if df_source.count() == 0:
    print("--- No new data to process. Exiting. ---")
    sys.exit(0)

# -------------------------------------------------------------------------
# 5. TRANSFORMATIONS (Rename & PII)
# -------------------------------------------------------------------------
# We build the select expression dynamically
select_exprs = []

for src_col in source_columns:
    # If we know this column from metadata, apply rules
    if src_col in column_mapping:
        target_name = column_mapping[src_col]['target']
        is_pii = column_mapping[src_col]['is_pii']
        
        col_obj = F.col(src_col)
        
        # PII Logic: If True, hash it (SHA256)
        if is_pii:
            col_obj = F.sha2(col_obj, 256)
            
        select_exprs.append(col_obj.alias(target_name))
        
    else:
        # Schema Evolution: If it's a new column not in metadata, keep it as is (Pass-through)
        # OR you could ignore it. Here we pass it through.
        select_exprs.append(F.col(src_col))

df_transformed = df_source.select(*select_exprs)

# -------------------------------------------------------------------------
# 6. WRITE TO BRONZE
# -------------------------------------------------------------------------
# Output path: s3://bucket/bronze/{table_name}/
bronze_path = f"s3://vivek-weather-datalake/bronze/{table_name_param}/"

print(f"--- Writing Data to {bronze_path} ---")

df_transformed.write.mode("append").parquet(bronze_path)

# -------------------------------------------------------------------------
# 7. UPDATE WATERMARK (The "State")
# -------------------------------------------------------------------------
# Find the max date in the batch we just processed
if watermark_col:
    # Note: We need to use the TARGET name of the watermark column now
    target_watermark_col = column_mapping[watermark_col]['target']
    
    max_date_row = df_transformed.agg(F.max(target_watermark_col).alias("max_val")).collect()[0]
    new_watermark = max_date_row["max_val"]
    
    if new_watermark:
        print(f"--- Updating Watermark to: {new_watermark} ---")
        
        # We use a pure JDBC update here.
        # Since Spark generic JDBC writer is for INSERT, for UPDATE we usually use a custom function
        # or a quick driver hack. For standard Glue, the cleanest way is often overwriting a 1-row temp table
        # and running a stored proc, OR just using a raw connection.
        
        # SIMPLIFIED APPROACH FOR DEMO: 
        # In Prod, you'd use pg8000 or psycopg2. Here, we print the SQL for you to verify manually first.
        # Because Spark doesn't support "UPDATE WHERE" natively via JDBC write.
        
        print(f"PLEASE EXECUTE DB UPDATE: UPDATE control_plane.bronze_table_details SET last_watermark_value = '{new_watermark}' WHERE table_id = {table_id}")

job.commit()