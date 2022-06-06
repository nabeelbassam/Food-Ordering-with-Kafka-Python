import email
from ensurepip import bootstrap
import json
from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer


ORDER_CONFIRMED_KAFKA_TOPIC = "order-confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

total_orders_count = 0
total_revenue = 0

print("Analytics consumer start listening ..." )

while True:
    for message in consumer:
        consumed_message = json.loads(message.)

