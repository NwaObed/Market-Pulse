from kafka import KafkaConsumer
import json
import time


# --- Configuration matching the Producer ---


consumer = KafkaConsumer(
    'stock_analysis',
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='market-pulse-consumer-group', # Define a consumer group
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting kafka consumer. Waiting for message on topic 'customer_info'...")

for message in consumer:
    data = message.value

    # Print received data

    print(f" Value (Deserialized): {data}")

consumer.close()
print("Kafka consumer closed.")