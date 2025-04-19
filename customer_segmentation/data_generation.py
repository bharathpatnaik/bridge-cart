import random, json, math, numpy as np
from datetime import timedelta
from faker import Faker
from kafka import KafkaProducer

# P(category)  – groceries dominate, sports is niche
CAT_WEIGHTS = {
    "Groceries": 0.30, "Electronics": 0.22, "Clothing": 0.18,
    "Books": 0.12, "Home Decor": 0.10, "Sports": 0.08,
}
PAY_WEIGHTS = {
    "Credit Card": 0.4, "UPI": 0.25, "Net Banking": 0.15,
    "PayPal": 0.12, "COD": 0.08,
}

fake = Faker()


def _choose(mapping):
    keys, probs = zip(*mapping.items())
    return np.random.choice(keys, p=probs)


def _age_income():
    age = int(np.random.normal(35, 12))  # 68% between 23‑47
    age = min(max(age, 18), 75)
    base = 18_000 + 1_000 * age  # weak correlation
    income = int(np.random.lognormal(mean=math.log(base), sigma=0.35))
    return age, min(income, 300_000)


def _purchase_amt(cat):
    # log‑normal makes long tail; electronics & sports cost more
    bump = {"Electronics": 1.4, "Sports": 1.25}.get(cat, 1)
    return round(float(np.random.lognormal(3.5, 0.6) * bump), 2)


def _coupon(cat, amt):
    # groceries & books coupon‑heavy, high tickets less likely
    base_p = 0.4 if cat in {"Groceries", "Books"} else 0.25
    p = base_p * (1 if amt < 150 else 0.4)
    used = random.random() < p
    disc = round(amt * random.uniform(0.05, 0.35), 2) if used else 0.0
    return used, disc


def generate_kafka_data(topic: str, bootstrap_servers: str, n_records: int) -> None:
    """
    Generates synthetic e-commerce transaction data and sends it to a Kafka topic.

    This function simulates customer purchases with realistic distributions for
    age, income, and purchase amounts. It also randomly assigns coupon usage,
    discounts, product categories, payment methods, and timestamps. Highlights:

    1. **Customer Profile**:
       - A unique `customer_id` (UUID) and `name` are generated using the Faker library.
       - `age` and `income` have a weak positive correlation, with age drawn from a
         normal distribution (18–75) and income from a log-normal distribution
         (capped at ₹3,00,000).
       - `gender` is randomly chosen between 'M' or 'F'.
       - `mobile` is created via Faker phone numbers.

    2. **Purchase Context**:
       - **Product Category** is chosen from weighted probabilities (`CAT_WEIGHTS`),
         making groceries the most common category and sports the least.
       - **Payment Method** is similarly chosen from weighted options (`PAY_WEIGHTS`).
       - **Purchase Timestamps** are sampled within the last 30 days, with a small
         chance of clustering due to “flash-sale” bursts (±1 hour).

    3. **Transaction Details**:
       - **purchase_amount** is sampled from a log-normal distribution with slight
         category-based adjustments (e.g., Electronics cost more).
       - **coupon_used** and **discount_amount** are determined by category,
         purchase amount, and random chance, simulating real-life coupon usage.
       - The final record includes all fields (customer attributes, payment info,
         product category, discount, etc.).

    4. **Kafka Integration**:
       - Each record is serialized to JSON and sent to the specified Kafka `topic`.
       - The function produces `n_records` total, flushes them to ensure delivery,
         then closes the producer connection.

    :param topic: Name of the Kafka topic to which data will be published.
    :param bootstrap_servers: Comma-separated list of Kafka server addresses.
    :param n_records: Number of synthetic transaction records to generate and send.
    :return: None
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for _ in range(n_records):
        cid = fake.uuid4()
        name = fake.name()
        age, income = _age_income()
        gender = random.choice(["M", "F"])
        mobile = fake.phone_number()

        cat = _choose(CAT_WEIGHTS)
        pay = _choose(PAY_WEIGHTS)
        dt = fake.date_time_between(start_date="-30d", end_date="now")
        # small cluster of “flash‑sale” bursts
        if random.random() < 0.05:
            dt += timedelta(hours=random.randint(-1, 1))

        amt = _purchase_amt(cat)
        coupon_used, discount = _coupon(cat, amt)

        record = {
            "customer_id": cid, "name": name, "age": age, "income": income,
            "gender": gender, "mobile": mobile, "purchase_date": dt.isoformat(),
            "purchase_amount": amt, "coupon_used": coupon_used,
            "discount_amount": discount, "payment_method": pay,
            "product_category": cat,
        }
        producer.send(topic, record)
    producer.flush()
    producer.close()
