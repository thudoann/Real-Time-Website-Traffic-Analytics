import os
from confluent_kafka import Producer
# Supposons que la variable d'environnement s'appelle KAFKA_PASSWORD
kafka_password = os.getenv("SASL_PASSWORD")


def lire_config_kafka(fichier_config):
    config = {}
    with open(fichier_config, 'r') as file:
        for line in file:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                config[key] = value

    config["sasl.password"]= kafka_password
    return config


def instance_kafka(config_kafka):
    p = Producer({
        'bootstrap.servers': config_kafka.get('bootstrap.servers'),
        'security.protocol': config_kafka.get('security.protocol'),
        'sasl.mechanisms': config_kafka.get('sasl.mechanisms'),
        'sasl.username': config_kafka.get('sasl.username'),
        'sasl.password': kafka_password
    })
    return p