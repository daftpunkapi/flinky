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


# Define WebSocket message handler
def on_message(ws, message):
    # Parse message as JSON
    data = json.loads(message)

    # Extract relevant data fields
    price = data['p']
    volume = data['v']
    symbol = data['s']

    # Create JSON payload
    payload = {
        'symbol': symbol,
        'price': price,
        'volume': volume
    }

    # Convert payload to JSON string
    json_payload = json.dumps(payload)

    # Produce message to Kafka topic
    producer.produce(kafka_topic, value=json_payload, callback=delivery_report)


# Create WebSocket connection and subscribe to endpoint
websocket.enableTrace(True)
ws = websocket.WebSocketApp('wss://ws.finnhub.io?token=ch4bj8hr01qlenl5r220ch4bj8hr01qlenl5r22g',
                            on_message=on_message)
ws.on_open = lambda ws: ws.send(json.dumps({'type':'subscribe', 'symbol': 'AAPL'}))
ws.run_forever()