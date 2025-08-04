
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('data/orders.csv', 'r') as file:
    next(file)
    for line in file:
        producer.send('orders_topic', value=line.strip().encode('utf-8'))
        time.sleep(0.1)

producer.flush()
