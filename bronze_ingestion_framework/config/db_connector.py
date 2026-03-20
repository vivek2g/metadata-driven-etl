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
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
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