from confluent_kafka import Producer
from pyspark.sql import SparkSession
import json
import time
import os

# Kafka broker configuration
KAFKA_BROKER = "localhost:9092"

# Define dataset-topic mapping
datasets = {
    "/Users/sandeep/Documents/lendingclubproject/bad/bad_customer_data_final/part-00000-cb23364b-0615-4c29-8445-d961d15be9b3-c000.csv": "dataset_1",
    "/Users/sandeep/Documents/lendingclubproject/bad/bad_data_customers/part-00000-d1d4d7e7-b456-4385-ac86-0261b5d2da47-c000.csv": "dataset_2",
    "/Users/sandeep/Documents/lendingclubproject/bad/bad_data_loans_defaulters_delinq/part-00000-c332b920-a8e1-4331-9586-8efbe2838d86-c000.csv": "dataset_3",
    "/Users/sandeep/Documents/lendingclubproject/bad/bad_data_loans_defaulters_detail_rec_enq/part-00000-c7439dbc-e0b4-4613-a1f8-266d45742b7a-c000.csv": "dataset_4"
}

# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Initialize Kafka Producer (Using confluent-kafka)
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Function to send message to Kafka
def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Read and send each dataset row-by-row
for file_path, topic in datasets.items():
    if not os.path.exists(file_path):
        print(f"File Not Found: {file_path}")
        continue  # Skip this file

    try:
        print(f"Reading file: {file_path}")
        df = spark.read.csv(file_path, header=True, inferSchema=True)  # Load CSV using Spark
        df_json = df.toJSON().collect()  # Convert Spark DataFrame to JSON records

        for record in df_json:
            producer.produce(topic, value=json.dumps(json.loads(record)), callback=delivery_report)
            producer.poll(0)  # Allow background processing
            time.sleep(1)  # Simulating real-time streaming

    except Exception as e:
        print(f"Error Processing {file_path}: {e}")

producer.flush()
print("Kafka Producer Finished Sending Data!")
