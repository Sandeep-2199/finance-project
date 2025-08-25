from confluent_kafka import Consumer, KafkaError
import json
import time
import os

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPICS = ['dataset_1', 'dataset_2', 'dataset_3', 'dataset_4']
CONSUMER_GROUP = 'lending-club-consumer-group'

# Consumer configuration
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'max.poll.interval.ms': 300000,
    'session.timeout.ms': 10000
}

# Create Consumer instance
consumer = Consumer(consumer_config)

# Subscribe to topics
consumer.subscribe(TOPICS)

# Counter for metrics
message_count = 0
start_time = time.time()

def process_message(topic, message_data):
    """
    Process the received message - add your transformation logic here
    """
    print(f"Received message from {topic}")
    
    # Example transformation
    transformed_data = message_data.copy()
    transformed_data['processed_timestamp'] = int(time.time() * 1000)
    transformed_data['consumer_group'] = CONSUMER_GROUP
    
    # Add topic-specific processing
    if topic == 'dataset_1':
        transformed_data['data_type'] = 'customer_data'
    elif topic == 'dataset_2':
        transformed_data['data_type'] = 'loan_data' 
    elif topic == 'dataset_3':
        transformed_data['data_type'] = 'defaulter_data'
    elif topic == 'dataset_4':
        transformed_data['data_type'] = 'repayment_data'
    
    return transformed_data

def save_to_database(data):
    """
    Save processed data to your database - implement your storage logic here
    """
    # Example: Print to console (replace with actual database insert)
    print(f"Saving to DB: {data['data_type']} with ID: {data.get('id', 'N/A')}")
    
    # Actual implementations might include:
    # - Writing to PostgreSQL/MySQL
    # - Saving to Elasticsearch  
    # - Storing in MongoDB
    # - Writing to a data warehouse
    pass

def save_to_file(topic, data):
    """
    Alternative: Save processed data to topic-specific files
    """
    output_dir = "processed_data"
    os.makedirs(output_dir, exist_ok=True)
    
    from datetime import datetime
    filename = f"{output_dir}/{topic}_{datetime.now().strftime('%Y%m%d')}.jsonl"
    
    with open(filename, 'a') as f:
        f.write(json.dumps(data) + '\n')
    
    return filename

try:
    print("Starting Kafka Consumer")
    print(f"Listening to topics: {TOPICS}")
    print("Press Ctrl+C to stop the consumer")
    print("")
    
    while True:
        # Poll for messages with timeout
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of {msg.topic()} partition {msg.partition()}")
            else:
                print(f"Consumer error: {msg.error()}")
            continue

        # Successfully received a message
        try:
            # Decode the message
            message_data = json.loads(msg.value().decode('utf-8'))
            message_count += 1
            
            # Process the message
            processed_data = process_message(msg.topic(), message_data)
            
            # Choose one saving method:
            
            # Option 1: Save to database
            save_to_database(processed_data)
            
            # Option 2: Save to file (uncomment below)
            # output_file = save_to_file(msg.topic(), processed_data)
            # print(f"Saved to file: {output_file}")
            
            # Print progress every 100 messages
            if message_count % 100 == 0:
                elapsed_time = time.time() - start_time
                rate = message_count / elapsed_time if elapsed_time > 0 else 0
                print(f"Processed {message_count} messages | Rate: {rate:.2f} msg/sec")
            
            # Commit offset manually
            consumer.commit(async=False)
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"Processing error: {e}")

except KeyboardInterrupt:
    print("Consumer interrupted by user")
    
finally:
    # Clean up on exit
    print("Cleaning up consumer")
    consumer.close()
    
    # Print final statistics
    elapsed_time = time.time() - start_time
    print("")
    print("Final Statistics:")
    print(f"Total messages processed: {message_count}")
    print(f"Total time: {elapsed_time:.2f} seconds")
    if elapsed_time > 0:
        print(f"Average rate: {message_count/elapsed_time:.2f} messages/second")
    print("Consumer stopped successfully")