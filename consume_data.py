import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import json
from kafka import KafkaConsumer

# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

if __name__=="__main__:
    # Fetch data from Kafka on aggregated votes per candidate
    consumer = create_kafka_consumer("top5_most_bought_product_name")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)
    results.head()