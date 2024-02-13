from confluent_kafka import Producer
import requests
import json
import time
import os 
def produce(max_produced, topic = 'logs_of_visitors'):
    # Configure the Kafka Producer
    producer_config = {
        'bootstrap.servers': 'pkc-60py3.europe-west9.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET')
    }
    producer = Producer(producer_config)
    # Fetch data from the continuous stream link and produce it to Kafka
    stream_link = 'https://api-56ce4d5f-779f448b-dku.eu-west-3.app.dataiku.io/public/api/v1/generate_rows/generate-rows/run'
    produced = 0
    while True and produced < max_produced:
        response = requests.get(stream_link)
        json_data = response.json()
        # Produce the JSON data to the Kafka topic
        producer.produce(topic, key=None, value=json.dumps(json_data))

        # Flush the producer to make sure all messages are sent
        producer.flush()
        produced += 1
        if topic == 'logs_of_visitors':
            time.sleep(1)
        else:
            time.sleep(3)
if __name__ == '__main__':
    produce(10)