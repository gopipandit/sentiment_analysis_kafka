from confluent_kafka import Producer, KafkaError
import os
from dotenv import load_dotenv
import streamlit as st
from datetime import datetime
import json


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



producer = Producer(conf)

topic = os.getenv("topic_in")

st.title("Sentiment Analysis App!")
st.subheader("Check the sentiment of a text")

st.write("Enter your sentence to check the sentiment of the input text.")
user_input = st.text_area("Enter here", height=150)

if st.button("Submit"):
    if user_input:
        message = {
            "text" : user_input,
            'timestamp' : datetime.now().isoformat()
        }

        producer.produce(topic,key = 'sentiment', value = json.dumps(message))
        producer.flush()
        st.success("Sent to Kafka!")

