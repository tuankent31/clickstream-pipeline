import json
import random
import time
from kafka import KafkaProducer

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost9092',
    value_serializer=lambda v json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = 'clickstream_events'
pages = ['home', 'products1', 'products2', 'cart', 'checkout']
events = ['page_view', 'add_to_cart']

print(Starting to send events...)
try
    while True
        event_data = {
            'user_id' f'user_{random.randint(1, 100)}',
            'page_url' random.choice(pages),
            'event_type' random.choice(events),
            'timestamp' int(time.time()  1000)
        }
        producer.send(KAFKA_TOPIC, event_data)
        print(fSent {event_data})
        time.sleep(random.uniform(0.5, 2))
except KeyboardInterrupt
    print(Stopping producer.)
finally
    producer.close()