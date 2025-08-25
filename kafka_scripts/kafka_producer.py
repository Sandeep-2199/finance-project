from confluent_kafka import Producer
from pyspark.sql import SparkSession
import json
import time
import os

# Kafka broker configuration
KAFKA_BROKER = "localhost:9092"

# Define dataset-topic mapping
datasets = {
    "/Users/sandeep/Documents/lendingclubproject/raw/customers_data_csv/part-00000-694b65c5-0b8f-4760-ac12-2b59978292e0-c000.csv": "dataset_1",
    "/Users/sandeep/Documents/lendingclubproject/raw/loans_data_csv/part-00000-30d34559-cada-4e5a-91c1-381f533f4c78-c000.csv": "dataset_2",
    "/Users/sandeep/Documents/lendingclubproject/raw/loans_defaulters_csv/loans_defaulters.csv": "dataset_3",
    "/Users/sandeep/Documents/lendingclubproject/raw/loans_repayments_csv/loans_repayments.csv": "dataset_4"
}

# Initialize Spark Session with more memory
spark = (
    SparkSession.builder
    .appName("KafkaProducer")
    .config("spark.driver.memory", "4g")  # Increase driver memory
    .config("spark.executor.memory", "2g")  # Increase executor memory
    .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
    .getOrCreate()
)

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Function to send message to Kafka
def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Process each dataset
for file_path, topic in datasets.items():
    if not os.path.exists(file_path):
        print(f"File Not Found: {file_path}")
        continue

    try:
        print(f"Reading file: {file_path}")
        
        # Read CSV in batches using Spark streaming approach
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        # Get the number of rows to process in batches
        total_rows = df.count()
        batch_size = 1000  # Process 1000 rows at a time
        
        print(f"Processing {total_rows} rows in batches of {batch_size}...")
        
        # Process data in batches to avoid memory issues
        for i in range(0, total_rows, batch_size):
            end_index = min(i + batch_size, total_rows)
            batch_df = df.limit(batch_size).offset(i)
            
            # Convert batch to JSON and process
            json_records = batch_df.toJSON().collect()
            
            for record in json_records:
                producer.produce(topic, value=record, callback=delivery_report)
                producer.poll(0)
                time.sleep(0.01)  # Reduced sleep for better throughput
            
            print(f"Processed rows {i} to {end_index-1} for topic {topic}")
            
            # Flush after each batch to ensure messages are sent
            producer.flush()
            
        print(f"Finished processing {total_rows} rows for topic {topic}")

    except Exception as e:
        print(f"Error Processing {file_path}: {e}")
        # Continue with next file even if one fails
        continue

producer.flush()
print("Kafka Producer Finished Sending Data!")
spark.stop()