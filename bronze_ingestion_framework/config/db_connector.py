import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    """
    Establishes the connection to the control_plane Postgres database.
    """
    try:
        # We use simple local credentials for testing. 
        # In AWS Glue, this would pull from Secrets Manager.
        conn = psycopg2.connect(
            host="control-plane.c0xcmc2qiesm.us-east-1.rds.amazonaws.com",
            database="etl_framework",
            user="postgres",
            password="glamf103", #need to be hardcoded for testing, but should be stored securely in production using secret manager
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        raise e

def fetch_schema_mapping(db_connection, dataset_id):
    """
    Pulls the exact column expectations for our dataset.
    """
    cursor = db_connection.cursor(cursor_factory=RealDictCursor)
    query = """
        SELECT source_column_name, target_column_name, target_data_type,
               is_mandatory, is_primary_key, is_pii
        FROM control_plane.bronze_schema_mapping
        WHERE dataset_id = %s
    """
    cursor.execute(query, (dataset_id,))
    metadata = cursor.fetchall()
    cursor.close()
    
    return metadata