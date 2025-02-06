import psycopg2

def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database along with a cursor.

    Returns:
        tuple: (conn, cursor) if successful, or (None, None) if the connection fails.
    """
    try:
        # Create a new connection to the PostgreSQL database using the provided credentials
        conn = psycopg2.connect(
            dbname="kafka_data",
            user="admin",
            password="admin123",
            host="postgres",
            port=5432
        )
        # Create a cursor from the connection for executing SQL commands
        cursor = conn.cursor()
        print("Database connection established.")
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None, None

def insert_into_db(cursor, timestamp, stock_price, sales_trend):
    """
    Insert a new record into the 'synthetic_data' table in PostgreSQL.

    Args:
        cursor (psycopg2.cursor): The database cursor used to execute SQL statements.
        timestamp (str or datetime): The timestamp of the record.
        stock_price (float): The stock price value.
        sales_trend (float): The sales trend value.
    """
    try:
        # Execute the INSERT statement with the provided values
        cursor.execute(
            "INSERT INTO synthetic_data (timestamp, stock_price, sales_trend) VALUES (%s, %s, %s)",
            (timestamp, stock_price, sales_trend)
        )
        # Commit the transaction to persist the changes in the database
        cursor.connection.commit()
        print(f"Inserted into DB: Timestamp={timestamp}, stock_price={stock_price}, sales_trend={sales_trend}")
    except Exception as e:
        print(f"Error inserting into DB: {e}")
