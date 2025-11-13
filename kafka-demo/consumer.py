from kafka import KafkaConsumer

KAFKA_TOPIC = "demo-topic"
KAFKA_SERVER = "localhost:9092"

def consumer_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Waiting for message...")
    for message in consumer:
        email = message.value.decode("utf-8")
        print(f"New Signup with Email: {email}")

if __name__ == "__main__":
    consumer_from_kafka()
