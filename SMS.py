import json
from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer


ORDER_CONFIRMED_KAFKA_TOPIC = "order-confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

print("SMS consumer start listening ...")

SMS_sent_until_now = set()
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_phone_number = consumed_message["customer_phone_number"]
        print(f"sending SMS to {customer_phone_number}")
        SMS_sent_until_now.add(customer_phone_number)
        print(f"SMS sent to {customer_phone_number}")
         
         
