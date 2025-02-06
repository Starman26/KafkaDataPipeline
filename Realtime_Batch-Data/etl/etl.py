import pandas as pd
from sqlalchemy import create_engine
from database import database
import warnings

warnings.filterwarnings('ignore')

def run_etl_process():
    """
    Execute the ETL process to extract data from the 'synthetic_data' table,
    transform it to calculate summaries, and load the results into summary tables
    in PostgreSQL.

    Process Overview:
      1. Establish a connection to the PostgreSQL database.
      2. Extract all rows from the 'synthetic_data' table into a DataFrame.
      3. Transform the data:
           - Convert the 'timestamp' column to datetime.
           - Drop the unique 'id' column.
           - Remove any duplicate rows.
           - Calculate a 10-minute average summary for the 'stock_price' column.
           - Calculate a 20-minute average summary for the 'sales_trend' column.
      4. Load the transformed summaries into PostgreSQL:
           - Save the stock price summary into the 'stock_price_summary' table.
           - Save the sales trend summary into the 'sales_trend_summary' table.
      5. Close the database connection.
    """
    print("The ETL process has been scheduled to run at 5-minute intervals.")

    # 1. Establish database connection
    print("Establishing connection to the database...")
    conn, cursor = database.get_db_connection()

    if not conn:
        print("Failed to connect to the database. Exiting ETL process.")
        return

    try:
        # 2. Extract data from the 'synthetic_data' table
        print("Extracting data from 'synthetic_data' table...")
        query = "SELECT * FROM synthetic_data"
        df = pd.read_sql(query, conn)
        print(f"Extracted {len(df)} rows from the database.")

        if df.empty:
            print("No data found in 'synthetic_data'. Exiting ETL process.")
            return

        # 3. Transform the data for summary calculations
        print("Transforming data for summaries...")
        print("Converting 'timestamp' column to datetime...")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        print(f"Total Missing Values: {df.isnull().sum().sum()}")

        print('Dropping the unique "id" column.')
        df.drop(columns=["id"], inplace=True)

        duplicated_count = sum(df.duplicated())
        print(f"Total duplicated rows: {duplicated_count}")
        if duplicated_count > 0:
            print(f"{duplicated_count} duplicate rows dropped.")
            df.drop_duplicates(inplace=True)

        # 3a. Calculate 10-minute summary for stock_price
        print("Calculating 10-minute summary for stock_price data...")
        stock_price_summary = (
            df.assign(interval_start=df['timestamp'].dt.floor('10min'))
              .groupby('interval_start')['stock_price']
              .mean()
              .reset_index()
              .rename(columns={'stock_price': 'avg_stock_price'})
        )

        # 3b. Calculate 20-minute summary for sales_trend
        print("Calculating 20-minute summary for sales_trend data...")
        sales_trend_summary = (
            df.assign(interval_start=df['timestamp'].dt.floor('20min'))
              .groupby('interval_start')['sales_trend']
              .mean()
              .reset_index()
              .rename(columns={'sales_trend': 'avg_sales_trend'})
        )

        print("Transformation complete. Summaries generated.")

        # 4. Load the transformed data into PostgreSQL
        print("Loading transformed data into PostgreSQL...")
        engine = create_engine('postgresql://admin:admin123@postgres:5432/kafka_data')

        print("Inserting stock_price summary data...")
        stock_price_summary.to_sql('stock_price_summary', engine, if_exists='replace', index=False)
        print(f"Inserted {len(stock_price_summary)} rows into 'stock_price_summary' table.")

        print("Inserting sales_trend summary data...")
        sales_trend_summary.to_sql('sales_trend_summary', engine, if_exists='replace', index=False)
        print(f"Inserted {len(sales_trend_summary)} rows into 'sales_trend_summary' table.")

        print("ETL Process Completed Successfully!")

    except Exception as e:
        print(f"Error during ETL process: {e}")

    finally:
        # 5. Clean up resources and close the database connection
        print("Closing database connection.")
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    run_etl_process()
