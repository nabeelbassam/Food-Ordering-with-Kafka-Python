import json
from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = "order-details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order-confirmed"

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

producer = KafkaProducer(
    bootstrap_servers = "localhost:29092"
) 

print("Transactions consumer start listening ...")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode)
        print(consumed_message)
        
        user_id = consumed_message["user-id"]
        total_cost = consumed_message["total_const"]
        
        data = {
            "customer_id" : user_id,
            "customer_phone_number" : f"07912345customer{user_id}",
            "customer_cost" : total_cost 
        }
        
        print("successful transation ...")
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        
        
        