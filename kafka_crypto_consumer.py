import json
import numpy as np
from collections import deque
from confluent_kafka import Consumer, Producer
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf

# Load the trained ML models
bitcoin_model = tf.keras.models.load_model('bitcoin_prediction_model.keras')
ethereum_model = tf.keras.models.load_model('ethereum_prediction_model.keras')

# Kafka configuration
consumer_conf = {
    'bootstrap.servers': 'SASL_SSL://pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'H43DZCJGXQP53UQI',
    'sasl.password': '6+Jor2ufexs0oSJ6LQscIW068mOkUUcmr59e0dg7MxuAPSMB7qMordT+lnpbk2Yw',
    'group.id': 'latest-crypto-price-predictorrr',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'SASL_SSL://pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'H43DZCJGXQP53UQI',
    'sasl.password': '6+Jor2ufexs0oSJ6LQscIW068mOkUUcmr59e0dg7MxuAPSMB7qMordT+lnpbk2Yw',
    'client.id': 'crypto-producer'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Subscribe to the 'crypto_prices' topic
consumer.subscribe(['crypto_prices'])

# Initialize the scaler (Assuming you used a MinMaxScaler during training)
scaler = MinMaxScaler(feature_range=(0, 1))

# Buffers to hold historical data
bitcoin_buffer = deque(maxlen=6)  # Reduced buffer size for testing
ethereum_buffer = deque(maxlen=6)  # Reduced buffer size for testing

def preprocess_data(data):
    # Preprocess data to match the model's expected input
    scaled_data = scaler.fit_transform(np.array(data).reshape(-1, 1))
    X = scaled_data.reshape(1, len(scaled_data), 1)  # Reshape to (1, time_steps, 1)
    return X

def send_prediction_to_kafka(prediction_data):
    # Convert all values to native Python float type
    def convert_to_float(obj):
        if isinstance(obj, np.float32):
            return float(obj)
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

    message = json.dumps(prediction_data, default=convert_to_float)
    producer.produce('predicted_price', key='prediction', value=message)
    producer.poll(1)

def make_prediction(model, prices):
    X = preprocess_data(prices)
    prediction = model.predict(X)
    predicted_price = scaler.inverse_transform(prediction)[0][0]
    return float(predicted_price)  # Convert to Python float

# Start consuming the data
try:
    while True:
        msg = consumer.poll(10.0)  # Poll for new messages
        if msg is None:
            print("No message received")
            continue
        
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize the message
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue

        # Check if the message contains an error code instead of actual data
        if 'status' in data['prices'] and 'error_code' in data['prices']['status']:
            print(f"Error in data: {data['prices']['status']['error_message']}")
            continue

        # Proceed if data is valid
        if 'bitcoin' in data['prices'] and 'ethereum' in data['prices']:
            bitcoin_price = data['prices']['bitcoin']['usd']
            ethereum_price = data['prices']['ethereum']['usd']
        else:
            print(f"Invalid data format: {data}")
            continue

        # Update historical buffers
        bitcoin_buffer.append(bitcoin_price)
        ethereum_buffer.append(ethereum_price)

        if len(bitcoin_buffer) == bitcoin_buffer.maxlen and len(ethereum_buffer) == ethereum_buffer.maxlen:
            # Make predictions
            bitcoin_pred = make_prediction(bitcoin_model, list(bitcoin_buffer))
            ethereum_pred = make_prediction(ethereum_model, list(ethereum_buffer))

            # Prepare and send the predictions to Kafka
            prediction_data = {
                'timestamp': data['timestamp'],
                'predicted_prices': {
                    'bitcoin': bitcoin_pred,
                    'ethereum': ethereum_pred
                }
            }
            send_prediction_to_kafka(prediction_data)

            print(f"Sent prediction: {prediction_data}")

except KeyboardInterrupt:
    pass
finally:
    # Clean up
    consumer.close()
