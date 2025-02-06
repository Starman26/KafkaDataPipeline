import psycopg2

try:
    conn = psycopg2.connect(
        dbname="kafka_data",
        user="admin",
        password="admin123",
        host="postgres",
        port=5432
    )
    print("Successful connection:", conn)
    conn.close()
except Exception as e:
    print("Connection error:", e)
