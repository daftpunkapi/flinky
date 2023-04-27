from confluent_kafka import Producer
import websocket
import json

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'stream1ws'

# Create Kafka producer
producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

# Define callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def on_message(ws, message):
    data = json.loads(message)
    data = data['data']
    filtered_data = [{'price':d['p'], 'time':d['t']} for d in data]
    filtered_data = json.dumps(filtered_data)
    print(filtered_data)

    # Produce message to Kafka topic
    producer.produce(kafka_topic, value=filtered_data, callback=delivery_report)


def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"NVDA"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=ch3mdhpr01qrc1e6u1k0ch3mdhpr01qrc1e6u1kg",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()