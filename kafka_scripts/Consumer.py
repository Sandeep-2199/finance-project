from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import logging
import signal
import sys
import importlib.util
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_consumer')

class KafkaMessageConsumer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', group_id: str = 'lending_club_group'):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.running = False
        self.consumer = None
        
        # Topics to consume from - these should match what your transformation files produce to
        self.topics = [
            'transformed_customers',      # from transformations/customer_data.py
            'transformed_loans',          # from transformations/loans_data.py  
            'transformed_defaulters',     # from transformations/loans_defaulters.py
            'transformed_repayments',     # from transformations/loans_repayments.py
        ]
        
        # Dynamically load transformation functions if needed
        self.transformation_functions = self.load_transformation_functions()

    def load_transformation_functions(self):
        """Dynamically load process functions from transformation modules"""
        functions = {}
        try:
            # Import and get the process function from each transformation module
            from transformations.customer_data import process_transformed_customer
            from transformations.loans_data import process_transformed_loan
            from transformations.loans_defaulters import process_transformed_defaulter
            from transformations.loans_repayments import process_transformed_repayment
            
            functions = {
                'transformed_customers': process_transformed_customer,
                'transformed_loans': process_transformed_loan,
                'transformed_defaulters': process_transformed_defaulter,
                'transformed_repayments': process_transformed_repayment,
            }
            logger.info("Successfully loaded all transformation functions")
            
        except ImportError as e:
            logger.warning(f"Could not import transformation functions: {e}")
            logger.warning("Using default processing methods")
        
        return functions

    def create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer"""
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        
        return Consumer(consumer_config)

    def process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Process message using the appropriate transformation function
        """
        try:
            # Use the dynamically loaded function if available
            if topic in self.transformation_functions:
                self.transformation_functions[topic](message)
            else:
                # Fallback to default processing
                self.default_message_processing(topic, message)
                
        except Exception as e:
            logger.error(f"Error processing message from topic {topic}: {e}")
            logger.error(f"Message content: {message}")

    def default_message_processing(self, topic: str, message: Dict[str, Any]) -> None:
        """Default processing if no specific function is found"""
        logger.info(f"Processing {topic}: {message.get('id', 'Unknown ID')}")
        
        # Add your default processing logic here
        # This could be writing to a database, cache, etc.
        
        if 'customer' in topic:
            logger.debug(f"Customer data: {message}")
        elif 'loan' in topic:
            logger.debug(f"Loan data: {message}")
        elif 'defaulter' in topic:
            logger.debug(f"Defaulter data: {message}")
        elif 'repayment' in topic:
            logger.debug(f"Repayment data: {message}")

    def start_consuming(self) -> None:
        """Start consuming messages from all topics"""
        self.consumer = self.create_consumer()
        self.consumer.subscribe(self.topics)
        self.running = True
        
        logger.info(f"Starting consumer for topics: {self.topics}")
        logger.info("Press Ctrl+C to stop...")
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    
                    logger.info(f"Received from '{msg.topic()}' [Partition: {msg.partition()}]")
                    
                    self.process_message(msg.topic(), message_value)
                    
                    # Commit offset after successful processing
                    self.consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Clean shutdown"""
        logger.info("Shutting down consumer...")
        self.running = False
        if self.consumer:
            self.consumer.close()

def signal_handler(sig, frame):
    logger.info("Interrupt signal received")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    consumer = KafkaMessageConsumer()
    consumer.start_consuming()