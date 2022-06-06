from ensurepip import bootstrap
from itertools import product
import json, time
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order-detials"
ORDER_LIMIT = 30

# producer = KafkaProducer(bootstrap_servers="localhost:29092")

# generate data
items = ["burger","sandwish","ice-cream"] * 10
for i,t in zip(range(1, ORDER_LIMIT),items):
    data = {"order_id": i + 100,
            "user_id": i+100,
            "total_cost":i+20, 
            "items":t}
    # producer.send(
    #     ORDER_KAFKA_TOPIC,
    #     json.dumps(data).encode("utf-8")
    # )
    # print(f"Sending {i} is done")
    # time.sleep(5)