
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('data/customers.csv', 'r') as file:
    next(file)  # Skip header
    for line in file:
        producer.send('customers_topic', value=line.strip().encode('utf-8'))
        time.sleep(0.1)

producer.flush()
