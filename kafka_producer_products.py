
from kafka import KafkaProducer
import pandas as pd
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

df = pd.read_excel('data/products.xlsx')

for _, row in df.iterrows():
    message = ','.join(map(str, row.values))
    producer.send('products_topic', value=message.encode('utf-8'))
    time.sleep(0.1)

producer.flush()
