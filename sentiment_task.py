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
}

# Create a producer instance
producer = Producer(conf)

# Topic to produce messages to
topic = os.getenv("topic")

es = Elasticsearch(os.getenv("end_point"),
                   basic_auth=("elastic", os.getenv("api_key_elasic")))


class SentimentAnalyzer:
    def __init__(self):
        self.consumer = Consumer(conf)
        self.producer = Producer(conf)
        self.input_topic = os.getenv("topic")
        self.output_topic = os.getenv("topic")

    def analyzeSentiment(self,text):
        analysis = TextBlob(text)
        return {
            'text' : text,
            'polarity' : analysis.sentiment.poloarity,
            'subjectivity' : analysis.sentiment.subjectivity,
            'timestamp' : datetime.now().isoformat()
        }
    
    def StoreIntoElastic(self, sentiment_data):
        es.index(index='sentiment_analysis', document= sentiment_data)
