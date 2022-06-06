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
        consumed_message = json.loads(message.value.decode())
        total_cost = consumed_message["total_cost"]
        
        total_orders_count +=1
        total_revenue += total_cost
        
        print(f"Orders count until now : {total_orders_count}")
        print(f"Total Orders revenue until now : {total_revenue}")
        
