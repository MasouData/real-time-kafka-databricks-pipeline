import time
from confluent_kafka import Producer
import json
import os

config={
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
    'client.id': 'transaction-producer'
}

producer = Producer(config)
topic = 'my_first_topic'

def produce_message():
    for i in range(10):
        key=f"key-{i}"
        value= json.dumps({"id": i, "message": f"sample message {i}"})
        print(f"Producing message : key={key} and value ={value}")
        producer.produce(
            topic=topic,
            key=key,
            value=value
        )
        producer.poll(0)
        time.sleep(1)
    producer.flush()

produce_message()