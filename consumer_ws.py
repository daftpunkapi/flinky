from kafka import KafkaConsumer

# consumer = KafkaConsumer('stream1ws')
consumer = KafkaConsumer('stream1ws', bootstrap_servers=['localhost:9092'],
auto_offset_reset='earliest', enable_auto_commit=False,
group_id = "ws_group",
auto_commit_interval_ms=1000)

print("Hello World")

for msg in consumer:
    print (msg)