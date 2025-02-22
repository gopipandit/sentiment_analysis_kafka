from confluent_kafka import Consumer, Producer, KafkaError
from elasticsearch import Elasticsearch
from textblob import TextBlob
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
conf = {
    'bootstrap.servers': os.getenv("bootstrap_server"), 
    'security.protocol': 'SASL_SSL',    
    'sasl.mechanisms': 'PLAIN',     
    'sasl.username': os.getenv("api_key"),     
    'sasl.password': os.getenv("api_secret"),
    'group.id': 'sentiment_analysis',
    'auto.offset.reset': 'earliest',
}

# Create a producer instance
producer = Producer(conf)

# Elasticsearch configuration
es = Elasticsearch(os.getenv("end_point"),
                   basic_auth=("elastic", os.getenv("api_key_elasic")))


class SentimentAnalyzer:
    def __init__(self):
        self.kafka_consumer = Consumer(conf)  # Renamed to avoid conflict
        self.kafka_producer = Producer(conf)  # Renamed for consistency
        self.input_topic = os.getenv("topic_in")
        self.output_topic = os.getenv("topic_out")

    def analyzeSentiment(self, text):
        analysis = TextBlob(text)
        return {
            'text': text,
            'polarity': analysis.sentiment.polarity,  # Fixed typo: poloarity -> polarity
            'subjectivity': analysis.sentiment.subjectivity,
            'timestamp': datetime.now().isoformat()
        }
    
    def storeIntoElastic(self, sentiment_data):
        es = Elasticsearch(
            os.getenv("end_point"), 
            api_key=os.getenv("encoded")
        )
        es.index(index='sentiment_analysis', document= sentiment_data)
        print("Successfully pushed to Elastic!")

    def sendToKafka(self, sentiment_data):
        try:
            value = json.dumps(sentiment_data).encode('utf-8')
            self.kafka_producer.produce(self.output_topic, value=value)
            self.kafka_producer.flush()
            print("Message Sent")
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")

    def start_consumer(self):  # Renamed to avoid conflict
        """
        Consume messages from the Kafka input topic and process them.
        """
        # Subscribe to the input topic
        self.kafka_consumer.subscribe([self.input_topic])
        print("Listening to the messages.............")

        try:
            while True:
                # Poll for messages (wait up to 1 second)
                msg = self.kafka_consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue  # No message received

                if msg.error():
                    # Handle errors
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"End of partition reached {msg.partition()}")
                    else:
                        print(f"Consumer Error: {msg.error()}")
                    continue

                try:
                    # Decode the message value
                    data = json.loads(msg.value().decode('utf-8'))
                    # print(f"Data to insert: {data}")  # Debug print

                    # Process the data (e.g., analyze sentiment, store in Elasticsearch, etc.)
                    sentiment_data = self.analyzeSentiment(data.get('text', ''))  # Assuming 'text' is a key in the data
                    self.storeIntoElastic(sentiment_data)
                    self.sendToKafka(sentiment_data)
                    print(f"Data to insert: {sentiment_data}")

                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                except KeyError as e:
                    print(f"Missing key in data: {e}")
                except Exception as e:
                    print(f"Error processing message: {e}")

        except KeyboardInterrupt:
            print("Consumer interrupted by user.")
        except Exception as e:
            print(f"Unexpected error in consumer: {e}")
        finally:
            # Close the consumer gracefully
            self.kafka_consumer.close()
            print("Consumer closed.")


# Main block
if __name__ == "__main__":
    # Create an instance of SentimentAnalyzer
    analyzer = SentimentAnalyzer()
    
    # Start consuming messages
    analyzer.start_consumer() 