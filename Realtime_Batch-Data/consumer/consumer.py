from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from database import database

def main():
    """
    Main function for the Kafka consumer that processes incoming messages and inserts data into PostgreSQL.

    The consumer performs the following steps:
      1. Configures and initializes a Kafka consumer.
      2. Establishes a database connection and obtains a cursor.
      3. Subscribes to the 'test_topic' Kafka topic.
      4. Polls for messages in an infinite loop.
         - If a message is received without errors, it decodes and parses the JSON payload.
         - Extracts the timestamp, stock_price, and sales_trend from the message.
         - Inserts the extracted data into the 'synthetic_data' table in PostgreSQL.
      5. Handles any Kafka or insertion errors.
      6. Cleans up and closes resources upon termination.
    """
    try:
        # Kafka consumer configuration
        conf = {
            'bootstrap.servers': 'kafka:29092',  # Kafka broker address within the Docker network
            'group.id': 'python-consumer',
            'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
        }
        
        # Initialize the Kafka Consumer
        consumer = Consumer(conf)
        topic = "test_topic"

        # Establish a connection to the PostgreSQL database and create a cursor
        conn, cursor = database.get_db_connection()
        if conn is None or cursor is None:
            print("Failed to establish database connection. Exiting consumer.")
            return

        # Subscribe to the Kafka topic
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")
        print("Consumer started. Waiting for messages...")

        # Consume messages in an infinite loop
        while True:
            # Poll for messages with a timeout of 1 second
            msg = consumer.poll(1.0)

            if msg is None:
                print("No message received. Polling...")
                continue

            if msg.error():
                # Handle errors from Kafka
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End-of-partition event (not an error condition)
                    print(f"End of partition reached {msg.partition()} at offset {msg.offset()}")
                else:
                    print(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
            else:
                # Process the valid message
                try:
                    # Decode and parse the message JSON
                    message = msg.value().decode('utf-8')
                    message_data = json.loads(message)

                    # Extract fields from the message
                    timestamp = message_data['timestamp']
                    stock_price = message_data['stock_price']
                    sales_trend = message_data['sales_trend']

                    # Insert the data into PostgreSQL
                    database.insert_into_db(cursor, timestamp, stock_price, sales_trend)

                except ValueError as e:
                    print(f"Error parsing message: {e}")

    except Exception as e:
        print("Error occurred:", e)

    finally:
        # Clean up resources before exiting
        print("Finished consuming messages.")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
