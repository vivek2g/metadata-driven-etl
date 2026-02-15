# Initial Pipeline Trigger - v1.0
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pg8000
import boto3
import re

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
    SELECT table_id, source_s3_path, target_s3_path, file_format, delimiter, has_header, 
           watermark_col_name, last_watermark_value, load_type, partition_cols
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
        "connectionName": db_connection_name,
        "dbtable": "control_plane.bronze_table_details",  # <-- The Dummy key to keep Glue working, we will override it with "query"
        "query": table_query                              # <-- The actual query Postgres will run
    }
).toDF()

if df_table_config.count() == 0:
    raise Exception(f"No active configuration found for table: {table_name_param}")

# Collect the config into a Python dictionary/Row
config = df_table_config.collect()[0]
table_id = config['table_id']
source_path = config['source_s3_path']
target_path = config['target_s3_path']
partition_cols_str = config['partition_cols']
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
        "connectionName": db_connection_name,
        "dbtable": "control_plane.bronze_column_details", # <-- The Dummy key to keep Glue working, we will override it with "query"
        "query": col_query                                # <-- The actual query Postgres will run
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

# --- NEW DEBUG & CLEANUP CODE ---
print(f"--- DEBUG: Raw columns seen by Spark: {df_source.columns} ---")

# Strip invisible BOM characters and leading/trailing spaces from all column names
cleaned_columns = [c.replace('\ufeff', '').strip() for c in df_source.columns]
df_source = df_source.toDF(*cleaned_columns)

print(f"--- DEBUG: Cleaned columns: {df_source.columns} ---")

# -------------------------------------------------------------------------
# 4. APPLY SCHEMA & VALIDATION + FEEDBACK LOOP
# -------------------------------------------------------------------------

source_columns = df_source.columns

# A. Check for REQUIRED columns based on the Data Contract 
for src_col, rules in column_mapping.items():
    if rules['required'] and src_col not in source_columns:
        raise Exception(f"SCHEMA ERROR: Missing required column '{src_col}' in source file.")

# B. The Schema Drift "Feedback Loop"
# Find columns that are in the source file, but NOT in our Postgres dictionary
postgres_known_columns = set(column_mapping.keys())
actual_source_columns = set(source_columns)
new_discovered_columns = actual_source_columns - postgres_known_columns

if new_discovered_columns:
    print(f"--- SCHEMA DRIFT DETECTED! Found new columns: {new_discovered_columns} ---")
    
    try:
        # Fetch secure connection details
        session = boto3.session.Session()
        glue_client = session.client('glue')
        conn_response = glue_client.get_connection(Name=args['METADATA_CONN_NAME'], HidePassword=False)
        conn_props = conn_response['Connection']['ConnectionProperties']
        
        match = re.search(r'jdbc:postgresql://([^:]+):(\d+)/(.+)', conn_props['JDBC_CONNECTION_URL'])
        host, port, database = match.groups()
        
        print("Connecting to metadata database to log new columns...")
        pg_conn = pg8000.connect(
            host=host, database=database, port=int(port),
            user=conn_props['USERNAME'], password=conn_props['PASSWORD']
        )
        cursor = pg_conn.cursor()
        
        # Insert each new column into the control plane
        insert_query = """
            INSERT INTO control_plane.bronze_column_details 
            (table_id, source_col_name, target_col_name, data_type, is_required, is_pii, is_auto_discovered) 
            VALUES (%s, %s, %s, %s, FALSE, FALSE, TRUE)
            ON CONFLICT (table_id, source_col_name) 
            DO UPDATE SET last_seen_date = CURRENT_TIMESTAMP;
        """
        
        # Convert Spark's dtypes list into a dictionary for easy lookup
        # e.g., {'employee_id': 'int', 'name': 'string'}
        source_dtypes = dict(df_source.dtypes)
        
        for col in new_discovered_columns:
            # 1. Ask Spark what the actual data type is
            spark_type = source_dtypes.get(col, 'string') # Default to string if not found
            
            # 2. (Optional but recommended) Map Spark types to standard SQL types for Postgres
            # You can expand this map based on your needs
            sql_type = 'VARCHAR'
            if spark_type in ['int', 'bigint', 'smallint']:
                sql_type = 'INTEGER'
            elif spark_type in ['double', 'float', 'decimal']:
                sql_type = 'NUMERIC'
            elif spark_type == 'boolean':
                sql_type = 'BOOLEAN'
            elif spark_type in ['timestamp', 'date']:
                sql_type = 'TIMESTAMP'

            # 3. Execute the insert using the dynamically mapped SQL type
            cursor.execute(insert_query, (table_id, col, col, sql_type))
            
            # Add the new column to our in-memory mapping so the rest of the script handles it
            column_mapping[col] = {
                'target': col,
                'is_pii': False,
                'required': False
            }
            
        pg_conn.commit()
        print(f"Successfully logged {len(new_discovered_columns)} new columns to the control plane.")
        
    except Exception as e:
        print(f"WARNING: Failed to log new columns to metadata: {str(e)}")
        if 'pg_conn' in locals():
            pg_conn.rollback()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'pg_conn' in locals(): pg_conn.close()

# C. Incremental Filter (Watermark CDC)
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
# 6. WRITE TO BRONZE (With Dynamic Partitioning)
# -------------------------------------------------------------------------
print(f"--- Writing Data to {target_path} ---")

writer = df_transformed.write.mode("append")

if partition_cols_str:
    # Convert the comma-separated string into a Python list
    # e.g., "city, department" -> ['city', 'department']
    partition_list = [col.strip() for col in partition_cols_str.split(',')]
    print(f"--- Partitioning data by: {partition_list} ---")
    
    # Apply the partitions
    writer = writer.partitionBy(*partition_list)

# Execute the write
writer.parquet(target_path)

# -------------------------------------------------------------------------
# 7. UPDATE WATERMARK (The "State") - JVM BRIDGING VERSION
# -------------------------------------------------------------------------
if watermark_col:
    target_watermark_col = column_mapping[watermark_col]['target']
    max_date_row = df_transformed.agg(F.max(target_watermark_col).alias("max_val")).collect()[0]
    new_watermark = max_date_row["max_val"]
    
    if new_watermark:
        print(f"--- DEBUG: Prepared to Update Watermark via JVM. Value: '{new_watermark}' ---")
        
        try:
            # 1. Fetch raw credentials (No parsing needed!)
            jdbc_conf = glueContext.extract_jdbc_conf(args['METADATA_CONN_NAME'])
            db_url = jdbc_conf['url']
            db_user = jdbc_conf['user']
            db_pass = jdbc_conf['password']
            
            # 2. Access the underlying Java Virtual Machine (JVM) via Py4J
            DriverManager = sc._gateway.jvm.java.sql.DriverManager
            
            # 3. Open connection natively using Spark's own JDBC driver
            print(f"--- DEBUG: Connecting natively to JVM URL: {db_url} ---")
            conn = DriverManager.getConnection(db_url, db_user, db_pass)
            
            # 4. Execute the update
            stmt = conn.createStatement()
            update_query = f"UPDATE control_plane.bronze_table_details SET last_watermark_value = '{new_watermark}' WHERE table_id = {table_id}"
            
            rows_affected = stmt.executeUpdate(update_query)
            print(f"--- DEBUG: Rows affected by JVM UPDATE: {rows_affected} ---")
            
            # 5. Force COMMIT
            if not conn.getAutoCommit():
                conn.commit()
                
            print(f"--- DEBUG: JVM Explicit COMMIT executed successfully! ---")
            
        except Exception as e:
            print(f"--- FAILED to update watermark via JVM: {str(e)} ---")
            
        finally:
            # Cleanly close Java resources
            if 'stmt' in locals() and stmt is not None: stmt.close()
            if 'conn' in locals() and conn is not None: conn.close()

# Tell AWS Glue the job is officially done
job.commit()