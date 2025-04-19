import random
import json
from faker import Faker
from kafka import KafkaProducer

def generate_kafka_data(topic: str, bootstrap_servers: str, n_records: int) -> None:
    """
    Generate synthetic e-commerce data and push to a Kafka topic.
    """
    fake = Faker()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for _ in range(n_records):
        customer_id = fake.uuid4()
        name = fake.name()
        age = random.randint(18, 75)
        income = random.randint(20_000, 150_000)
        gender = random.choice(["M", "F"])
        mobile = fake.phone_number()
        purchase_date = fake.date_time_between(start_date="-30d", end_date="now").isoformat()
        purchase_amount = round(random.uniform(5.0, 1000.0), 2)

        # Extended metrics
        coupon_used = random.random() < 0.3
        discount_amount = round(purchase_amount * random.uniform(0.05, 0.3), 2) if coupon_used else 0.0
        payment_method = random.choice(["Credit Card", "UPI", "Net Banking", "PayPal", "COD"])
        product_category = random.choice(["Electronics", "Books", "Clothing", "Home Decor", "Groceries", "Sports"])

        record = {
            "customer_id": str(customer_id),
            "name": name,
            "age": age,
            "income": income,
            "gender": gender,
            "mobile": mobile,
            "purchase_date": purchase_date,
            "purchase_amount": purchase_amount,
            "coupon_used": coupon_used,
            "discount_amount": discount_amount,
            "payment_method": payment_method,
            "product_category": product_category,
        }

        producer.send(topic, record)

    producer.flush()
    producer.close()
    print(f"Produced {n_records} records to Kafka topic '{topic}'")
