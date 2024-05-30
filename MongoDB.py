import logging
import pandas as pd
import json
from flask import Flask
from dash import Dash, html, dcc
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
from pymongo import MongoClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'topic1'

# Initialize Kafka Consumer with JSON deserializer
consumer = KafkaConsumer(
    topic_name,
    group_id='product_metadata_consumer_group',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MongoDB connection setup
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['local']
collection = db['kafka_data']

# Flask server setup for Dash app
server = Flask(__name__)
app = Dash(__name__, server=server)

# Define the layout of the dashboard
app.layout = html.Div([
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # in milliseconds
        n_intervals=0
    )
])

# Process incoming data to calculate mean price and store in MongoDB
def process_data(message):
    collection.insert_one(message)  # Insert the entire message into MongoDB
    if 'price' in message:
        return message['price']
    return None

# Update the graph periodically
@app.callback(Output('live-update-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    data = {
        'time': [],
        'mean_price': []
    }
    try:
        # Poll for a single message (non-blocking)
        message = next(consumer)
        if message is not None:
            mean_price = process_data(message.value)
            if mean_price is not None:
                data['time'].append(pd.Timestamp.now())
                data['mean_price'].append(mean_price)
    except StopIteration:
        logging.info("No new messages.")

    trace = go.Scatter(
        x=data['time'],
        y=data['mean_price'],
        name='Mean Price Over Time',
        mode='lines+markers'
    )

    return {
        'data': [trace],
        'layout': go.Layout(title='Real-Time Product Price Analysis')
    }

if __name__ == '__main__':
    app.run_server(debug=True)
