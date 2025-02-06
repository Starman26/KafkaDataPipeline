from confluent_kafka import Producer
import time
import pandas as pd
import json

def delivery_report(err, msg):
    """
    Callback function for Kafka Producer delivery reports.

    Args:
        err: An error object if an error occurred during message delivery; otherwise, None.
        msg: The Kafka message object.
    """
    if err:
        print(f"Failed to produce message to topic {msg.topic()} for event: {msg.key()}")
    else:
        val = msg.value().decode('utf8')
        print(f"Message: {val} sent to partition {msg.partition()}.")

# Read the dataset from a Parquet file
df = pd.read_parquet(r"data/synthetic_data.parquet", engine='pyarrow')
# Convert the 'timestamp' column to string format
df['timestamp'] = df['timestamp'].astype('str')

def main():
    """
    Main function for the Kafka Producer.

    This function reads synthetic data from a Parquet file and produces messages to a specified Kafka topic.
    Each row of the DataFrame is converted to a JSON payload and sent to the topic using the Producer.
    """
    # Producer configuration
    conf = {
        'bootstrap.servers': 'kafka:29092',  # Use the internal Kafka broker port
        'client.id': 'python-producer'
    }

    producer = Producer(conf)
    topic = "test_topic"

    print("Producing messages to Kafka...")
    # Iterate over each row in the DataFrame and produce a message
    for _, row in df.iterrows():
        # Create a message payload as a dictionary
        message = {
            "timestamp": row['timestamp'],
            "stock_price": row['stock_price'],
            "sales_trend": row['sales_trend']
        }
        # Serialize the message as a JSON string and encode it to bytes
        serialized_message = json.dumps(message).encode('utf-8')
        # Produce the message to the specified topic with the delivery callback
        producer.produce(topic, value=serialized_message, callback=delivery_report)
        # Optional: Introduce a small delay between messages if needed
        # time.sleep(1)
        # Flush the producer to ensure the message is sent before processing the next row
        producer.flush()

    print("Finished producing messages.")

if __name__ == "__main__":
    main()
