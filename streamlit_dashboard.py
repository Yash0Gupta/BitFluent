import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer
import plotly.graph_objects as go
from streamlit_lottie import st_lottie

# Kafka configuration
consumer_conf_predicted = {
    'bootstrap.servers': '<YOUR_BOOTSTRAP_SERVER>',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '<YOUR_API_KEY>',
    'sasl.password': '<YOUR_SECRET>',
    'group.id': 'predicted-dashboard-group-streamlit-updateddd',
    'auto.offset.reset': 'earliest'
}

consumer_conf_realtime = {
     'bootstrap.servers': '<YOUR_BOOTSTRAP_SERVER>',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '<YOUR_API_KEY>',
    'sasl.password': '<YOUR_SECRET>',
    'group.id': 'realtime-dashboard-group-streamlit-updateddd',
    'auto.offset.reset': 'earliest'
}

# Create consumers
consumer_predicted = Consumer(consumer_conf_predicted)
consumer_realtime = Consumer(consumer_conf_realtime)

consumer_predicted.subscribe(['predicted_price'])
consumer_realtime.subscribe(['crypto_prices'])

# Function to fetch data from Kafka
def fetch_predicted_prices():
    msg = consumer_predicted.poll(10.0)
    if msg is None or msg.error():
        print("No predicted data fetched")
        return None
    predicted_data = json.loads(msg.value().decode('utf-8'))
    print("Predicted data fetched:", predicted_data)
    return predicted_data

def fetch_realtime_prices():
    msg = consumer_realtime.poll(5.0)
    if msg is None or msg.error():
        print("No real-time data fetched")
        return None
    realtime_data = json.loads(msg.value().decode('utf-8'))
    print("Real-time data fetched:", realtime_data)
    return realtime_data

# Load historical data
df_historical = pd.read_csv('historical_prices.csv', parse_dates=['Date'], dayfirst=True)

# Function to load Lottie animations from a local file
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)

# Load the Lottie animation from the local file
crypto_animation = load_lottiefile('Animation - 1726245333722.json')

# Streamlit App Title and Layout
st.markdown("<h1 style='text-align:center; color:#ff6633;'>Cryptocurrency Dashboard</h1>", unsafe_allow_html=True)

# Animation display at the top
st_lottie(crypto_animation, height=150)

# Fetch data
predicted_data = fetch_predicted_prices()
realtime_data = fetch_realtime_prices()

# Layout: Real-time and Predicted Prices Section
st.markdown("<h2 style='color:#ff6633;'>Current and Predicted Prices</h2>", unsafe_allow_html=True)

if predicted_data and realtime_data:
    bitcoin_current = realtime_data['prices']['bitcoin']['usd']
    ethereum_current = realtime_data['prices']['ethereum']['usd']
    bitcoin_predicted = predicted_data['predicted_prices']['bitcoin']
    ethereum_predicted = predicted_data['predicted_prices']['ethereum']

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"""
        <div style='background-color:#b3d9ff;padding:15px;border-radius:10px;text-align:center;'>
            <h4 style='color:#555;'>Bitcoin</h4>
            <p style='color:#555;'>Current Price: <b>{bitcoin_current} USD</b></p>
            <p style='color:#555;'>Predicted Price: <b>{bitcoin_predicted} USD</b></p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style='background-color:#b3b3ff;padding:15px;border-radius:10px;text-align:center;'>
            <h4 style='color:#555;'>Ethereum</h4>
            <p style='color:#555;'>Current Price: <b>{ethereum_current} USD</b></p>
            <p style='color:#555;'>Predicted Price: <b>{ethereum_predicted} USD</b></p>
        </div>
        """, unsafe_allow_html=True)

    # Calculate and display the percentage differences and recommendations
    bitcoin_diff = (bitcoin_predicted - bitcoin_current) / bitcoin_current * 100
    ethereum_diff = (ethereum_predicted - ethereum_current) / ethereum_current * 100

    def buy_sell_recommendation(current_price, predicted_price):
        if predicted_price > current_price:
            return "BUY"
        elif predicted_price < current_price:
            return "SELL"
        else:
            return "HOLD"

    bitcoin_recommendation = buy_sell_recommendation(bitcoin_current, bitcoin_predicted)
    ethereum_recommendation = buy_sell_recommendation(ethereum_current, ethereum_predicted)

    st.markdown(f"""
    <div style='text-align:center;font-size:20px;color:#8BC34A;padding:10px;'>Bitcoin Recommendation: <b>{bitcoin_recommendation}</b> 
    ({bitcoin_diff:.2f}% difference)</div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style='text-align:center;font-size:20px;color:#FF5722;padding:10px;'>Ethereum Recommendation: <b>{ethereum_recommendation}</b> 
    ({ethereum_diff:.2f}% difference)</div>
    """, unsafe_allow_html=True)

else:
    st.warning("No data available")

# Historical Prices Section
st.markdown("<h2 style='color:#7300e6;'>Historical Prices</h2>", unsafe_allow_html=True)

# Plot the historical data
fig = go.Figure()

fig.add_trace(go.Scatter(x=df_historical['Date'], y=df_historical['bitcoin'], mode='lines', name='Bitcoin Historical',
                         line=dict(color='green')))
fig.add_trace(go.Scatter(x=df_historical['Date'], y=df_historical['ethereum'], mode='lines', name='Ethereum Historical',
                         line=dict(color='orange')))

fig.update_layout(
    title="Bitcoin and Ethereum Historical Prices",
    xaxis_title="Date",
    yaxis_title="Price (USD)",
    xaxis=dict(showline=True, showgrid=False),
    yaxis=dict(showline=True, showgrid=False),
    plot_bgcolor="white"
)

st.plotly_chart(fig)

# Weekly average deviation section
st.markdown("<h3 style='color:#7300e6;'>Weekly Price Deviation</h3>", unsafe_allow_html=True)

def calculate_weekly_average(current_price, historical_data):
    last_7_days = historical_data.tail(7)
    weekly_average = last_7_days.mean()
    deviation = (current_price - weekly_average) / weekly_average * 100
    return weekly_average, deviation

if realtime_data:
    bitcoin_weekly_avg, bitcoin_deviation = calculate_weekly_average(bitcoin_current, df_historical['bitcoin'])
    ethereum_weekly_avg, ethereum_deviation = calculate_weekly_average(ethereum_current, df_historical['ethereum'])

    st.markdown(f"<div style='font-size:18px;'>Bitcoin Weekly Average: {bitcoin_weekly_avg:.2f} USD "
                f"(Deviation: {bitcoin_deviation:.2f}%)</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:18px;'>Ethereum Weekly Average: {ethereum_weekly_avg:.2f} USD "
                f"(Deviation: {ethereum_deviation:.2f}%)</div>", unsafe_allow_html=True)

# Refresh Button
if st.button("Refresh"):
    st.experimental_rerun()

# Close the consumers after use
consumer_predicted.close()
consumer_realtime.close()
