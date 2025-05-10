import pandas as pd
import json
from kafka import KafkaProducer
import time

# Load data
df = pd.read_excel("products.xlsx")

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stream each product
for _, row in df.iterrows():
    message = {
        "id": str(row["ID"]),
        "category": row["Category"],
        "created_at": str(row["Created At"]),
        "ean": str(row["Ean"]),
        "price": float(row["Price"]),
        "rating": float(row["Rating"]),
        "title": row["Title"],
        "vendor": row["Vendor"]
    }
    producer.send("order-events", message)
    print(f"Sent: {message}")
    time.sleep(1)
