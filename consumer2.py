from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'order',
    bootstrap_servers='localhost:9092',
    group_id='order-group',
    auto_offset_reset='latest',   # 👈 only new messages
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode('utf-8'),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("Consumer 2 is running and waiting for messages...\n")

try:
    while True:
        for message in consumer:
            print(
                f"[RECEIVED] Key={message.key} | "
                f"Value={message.value} | "
                f"Partition={message.partition} | "
                f"Offset={message.offset}"
            )

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()