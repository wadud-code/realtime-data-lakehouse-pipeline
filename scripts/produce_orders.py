
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer targeting local broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'order_events'
customer_ids = [101, 102, 103, 104, 105]
items = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']

print(f"Starting order stream to topic '{topic_name}'...")

try:
    while True:
        order_event = {
            'order_id': random.randint(1000, 9999),
            'customer_id': random.choice(customer_ids),
            'item': random.choice(items),
            'amount': round(random.uniform(15.0, 1200.0), 2),
            'event_timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Send event to Kafka topic
        producer.send(topic_name, value=order_event)
        print(f"Produced: {order_event}")
        
        # Stream 1 message every second
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping stream producer.")
    producer.flush()
    producer.close()
