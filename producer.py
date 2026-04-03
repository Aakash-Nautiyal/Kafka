from kafka import KafkaProducer

# Custom partition logic
def region_partitioner(key_bytes, all_partitions, available_partitions):
    key = key_bytes.decode('utf-8')

    mapping = {
        "north": 0,
        "south": 1,
        "west": 2,
        "east": 3
    }

    return mapping.get(key, 0)  # default partition 0 if unknown


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8'),
    partitioner=region_partitioner
)

while True:
    region = input("Enter region (north/south/east/west): ")
    message = input("Enter message: ")

    producer.send(
        'order',
        key=region,
        value=message
    )

    print(f"Sent '{message}' to region '{region}'")

    producer.flush()