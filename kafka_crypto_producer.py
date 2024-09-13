import time
import json
import requests
from confluent_kafka import Producer

# Kafka Configurations
conf = {
    'bootstrap.servers': '<YOUR_BOOTSTRAP_SERVER>',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '<YOUR_API_KEY>',
    'sasl.password': '<YOUR_SECRET>',
    'client.id': 'crypto-producer'
}

# Initialize Producer
producer = Producer(conf)

# CoinGecko API to fetch cryptocurrency prices
api_url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd'

def delivery_report(err, msg):
    """ Called once for each message to indicate delivery result. Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Function to send crypto data to Kafka
def fetch_and_send_crypto_data():
    while True:
        try:
            # Fetch crypto prices from API
            response = requests.get(api_url)
            crypto_data = response.json()

            # Prepare Kafka message
            message = json.dumps({
                'timestamp': time.time(),
                'prices': crypto_data
            })

            # Send message to Kafka topic 'crypto_prices'
            producer.produce('crypto_prices', key='crypto', value=message, callback=delivery_report)
            producer.poll(1)

            print(f"Sent: {message}")

            # Sleep for a while before the next request
            time.sleep(10)

        except Exception as e:
            print(f"Error fetching or sending data: {e}")

if __name__ == "__main__":
    fetch_and_send_crypto_data()
