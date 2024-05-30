from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
import hashlib
import logging

# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'topic2'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    group_id='pcy_consumer_group',
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# PCY parameters
min_support = 0.1  # Adjust as needed
hash_table_size = 100  # Size of the hash table
batch_size = 2  # Transactions batch size for processing

# Initialize data structures
transactions = []
hash_table = [0] * hash_table_size
bitmap = [0] * hash_table_size

def hash_pair(pair):
    """Consistent hash function for item pairs."""
    pair_string = '+'.join(sorted(pair))
    return int(hashlib.sha256(pair_string.encode('utf-8')).hexdigest(), 16) % hash_table_size

def extract_items(message):
    """Extract items from Kafka message."""
    also_buy = message.get('also_buy', [])
    title = message.get('title', '')
    return [title] + also_buy if isinstance(also_buy, list) else [title]

def update_hash_table_and_bitmap(items):
    """Update hash table counts and dynamically update bitmap for the new item pairs."""
    for pair in combinations(set(items), 2):
        hash_index = hash_pair(pair)
        hash_table[hash_index] += 1
        # Update bitmap based on the new count
        if hash_table[hash_index] >= (min_support * len(transactions)):
            bitmap[hash_index] = 1

def find_frequent_pairs():
    """Find and print frequent item pairs using the bitmap."""
    candidate_pairs = defaultdict(int)
    for transaction in transactions:
        for pair in combinations(set(transaction), 2):
            if bitmap[hash_pair(pair)]:
                candidate_pairs[pair] += 1

    for pair, count in candidate_pairs.items():
        if count / len(transactions) >= min_support:
            logging.info(f"Frequent pair: {pair}, Count: {count}, Support: {count / len(transactions):.2f}")

# Consume messages from Kafka
for message in consumer:
    items = extract_items(message.value)
    transactions.append(items)

    # Process transactions if enough have accumulated
    if len(transactions) >= batch_size:
        update_hash_table_and_bitmap(items)
        find_frequent_pairs()

        # Reset for next batch
        transactions.clear()

        # Optionally reset hash table if starting fresh each batch
        # hash_table = [0] * hash_table_size


