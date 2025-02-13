from confluent_kafka import Consumer, KafkaError, KafkaException
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkTransformation") \
    .getOrCreate()

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['dataset_3'])

# Read data from Kafka
while True:
    msg = consumer.poll(1.0)  # Timeout of 1 second

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
        elif msg.error():
            raise KafkaException(msg.error())
    else:
        # Proper message
        dataset_3 = json.loads(msg.value().decode('utf-8'))

        # Convert the dataset to a Spark DataFrame
        df = spark.createDataFrame([dataset_3])
        
        # Perform your transformations here
        # Example transformation: select specific columns
        transformed_df = df.select(col("column1"), col("column2"))
        
        # Show the transformed data
        transformed_df.show()
        
        # Break after reading one message for demonstration purposes
        break

# Stop the Spark session
spark.stop()
consumer.close()