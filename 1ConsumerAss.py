from kafka import KafkaConsumer
from apyori import apriori
import json

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'topic1'

# Initialize Kafka consumer
consumer = KafkaConsumer(topic_name,
                         group_id='apriori_consumer_group',
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Apriori parameters
min_support = 0.1  # Adjust as needed
min_confidence = 0.5  # Adjust as needed
min_lift = 1.0  # Adjust as needed

# Function to extract items from Kafka message
def extract_items(message):
    also_buy = message.get('also_buy', [])
    title = message.get('title', '')
    return [title] + also_buy if isinstance(also_buy, list) else [title]

# List to store transactions
transactions = []

# Consume messages from Kafka
for message in consumer:
    # Extract items from Kafka message
    items = extract_items(message.value)
    transactions.append(items)

    # Apply Apriori if enough transactions accumulated
    if len(transactions) >= 100:  # Adjust threshold for practical batch size
        results = list(apriori(transactions, min_support=min_support, min_confidence=min_confidence, min_lift=min_lift))
        
        # Print association rules
        for result in results:
            for rule in result.ordered_statistics:
                print(f"Rule: {', '.join(rule.items_base)} -> {', '.join(rule.items_add)}")
                print(f"Confidence: {rule.confidence:.2f}")
                print(f"Lift: {rule.lift:.2f}")
                print("--------------------")
        
        # Clear transactions list
        transactions = []
