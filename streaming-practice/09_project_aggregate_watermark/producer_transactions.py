import time
import json
from faker import Faker
from kafka import KafkaProducer
import random
import uuid
import os
#Get Kafka adress from env configuration.
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"
#Produce fake event use faker and random
fake = Faker()
USER_IDS = list(range(1, 12))
GOODS = ["Laptop", "Phone", "Keyboard", "Mouse", "Monitor", "Charger", "Headset", "Desk Lamp"]
#Create fake event with 10% chance late 30 minutes - 1 hour.
def get_transaction_data():
    goods = random.choice(GOODS)
    qty = random.randint(1, 5)
    price = round(random.uniform(10.0, 200.0), 2)
    amount = round(qty * price, 2)
    #Create late timestamp.
    is_late = random.random() <0.1
    if is_late:
        past_offset = random.randint(1800,3600)
        event_time = int(time.time() - past_offset)
    else:
        event_time = int(time.time())
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "goods": goods,
        "quantity": qty,
        "price": price,
        "amount": amount,
        "timestamp": event_time,
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=json_serializer
)

KAFKA_TOPIC = "purchasing"

def produce_burst_of_purchasing(user_id, num_transactions):
    for _ in range(num_transactions):
        data = get_transaction_data()
        data["user_id"] = user_id  
        producer.send(KAFKA_TOPIC, data)
        print("Burst Sent:", data)
        time.sleep(random.uniform(0.1, 0.5))
#Initiate program. 
if __name__ == "__main__":
    try:
        while True:
            if random.random() < 0.005:
                user_to_burst = random.choice(USER_IDS)
                produce_burst_of_purchasing(user_to_burst, random.randint(6, 10))
            else:
                data = get_transaction_data()
                producer.send(KAFKA_TOPIC, data)
                print("Sent:", data)

            time.sleep(random.uniform(1, 4))
    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()