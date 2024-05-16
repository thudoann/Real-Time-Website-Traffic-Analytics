from confluent_kafka import Consumer, KafkaError
import json
import time
import pandas as pd
import geoip2.database
import os
from utils import lire_config_kafka

config_kafka = lire_config_kafka('./dashboard/kafka.config')

def consume(consumption_max_per_execution):
    ####################################################################
    ###################### kafka consumer ##############################
    ####################################################################
    # Configure the Kafka Consumer
    """consumer_config = {
        'bootstrap.servers': 'pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092',
        'group.id': 'your-consumer-group',  # Provide a valid and unique consumer group ID
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET')
    }

    consumer = Consumer(consumer_config)"""


        # Configuration de base du consommateur Kafka
    config_kafka['group.id'] = 'your-consumer-group'
    config_kafka['auto.offset.reset'] = 'earliest'

    # Créer une instance du consommateur Kafka
    consumer = Consumer({
        'bootstrap.servers': config_kafka.get('bootstrap.servers'),
        'security.protocol': config_kafka.get('security.protocol'),
        'sasl.mechanisms': config_kafka.get('sasl.mechanisms'),
        'sasl.username': config_kafka.get('sasl.username'),
        'sasl.password': config_kafka.get('sasl.password'),
        'group.id': config_kafka.get('group.id'),
        'auto.offset.reset': config_kafka.get('auto.offset.reset')

    })


    # Subscribe to the Kafka topic
    consumer.subscribe(['cr_classifier'])
    consumption_list = []
    current_consumption = 0
    # Poll for messages
    try:
        while True and current_consumption < consumption_max_per_execution:
            msg = consumer.poll(1.0)  # Adjust the timeout as needed

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the received JSON data
            json_data = json.loads(msg.value().decode('utf-8'))
            consumption_list.append(json_data['response'])
            current_consumption += 1
            time.sleep(2)
    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
    consumption_df = pd.DataFrame(consumption_list)
    print(('Consumed records: ' + str(len(consumption_df))))
    ##########################################################################################
    ################################ dashboard csv preparation ###############################
    ##########################################################################################
    # Load the CSV file
    def load_data():
        data = consumption_df.copy()
        # Optional: Parse server_ts as datetime
        data['server_ts'] = pd.to_datetime(data['server_ts'])
        return data
    data = load_data()
    data = data.drop(['br_width', 'br_height', 'sc_width', 'sc_height'], axis=1)
    data =data.rename(columns={"client_addr": "ip_address"})

    def is_valid_ipv4(ip):
        parts = ip.split(".")
        if len(parts) != 4:
            return False
        for part in parts:
            try:
                if not 0 <= int(part) <= 255:
                    return False
            except ValueError:
                return False
        return True

    # Filtering the DataFrame to only include valid IPv4 addresses, without adding a new column
    df = data[data['ip_address'].apply(is_valid_ipv4)]
    # Path to the GeoLite2 City database
    db_path = 'GeoLite2-City_20240130/GeoLite2-City.mmdb'
    # Function to extract geo-information
    def extract_geo_info(ip):
        try:
            with geoip2.database.Reader(db_path) as reader:
                response = reader.city(ip)
                country = response.country.name
                city = response.city.name
                latitude = response.location.latitude
                longitude = response.location.longitude
                return pd.Series([country, city, latitude, longitude])
        except Exception as e:
            print(f"Error: {e}")
            return pd.Series([None, None, None, None])

    # Apply the function to each IP address in the dataframe
    df[['country', 'city', 'latitude', 'longitude']] = df['ip_address'].apply(lambda ip: extract_geo_info(ip)).apply(pd.Series)
    # Extract year, month, and day
    df['year'] = df['server_ts'].dt.year
    df['month'] = df['server_ts'].dt.month
    df['day'] = df['server_ts'].dt.day

    df.to_csv('./dashboard/newlogs.csv', index=False)
    df = df[['country', 'city', 'latitude', 'longitude']]
    # Then perform the grouping and summing
    country_df = df.groupby(['country', 'latitude','longitude'])['city'].count().reset_index()
    country_df.to_csv('./dashboard/country.csv', index=False)

if __name__ == '__main__':
    print('Consuming and processing records...')
    consume(100)
    print('Consumed and processed records successfully')