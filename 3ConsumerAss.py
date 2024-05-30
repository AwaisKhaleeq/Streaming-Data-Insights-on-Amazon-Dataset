import json
from kafka import KafkaConsumer
from collections import defaultdict

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'topic1'  # Assuming we're using topic1 for our consumer

# Initialize Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    group_id='product_recommendation_group',
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Store frequently bought together products
frequently_bought_together = defaultdict(set)

def update_recommendations(data):
    """ Update the product recommendation set based on the also_buy field """
    asin = data['asin']
    also_buy = data.get('also_buy', [])
    
    # For each product that is bought with the current product, add the current product to their set
    for related_asin in also_buy:
        frequently_bought_together[related_asin].add(asin)
        frequently_bought_together[asin].add(related_asin)

def display_recommendations(asin):
    """ Display recommended products for a given product ID (asin) """
    recommendations = frequently_bought_together.get(asin, [])
    print(f"Recommendations for product {asin}: {recommendations}")

# Consume messages from Kafka
for message in consumer:
    data = message.value
    update_recommendations(data)
    
    # Optionally, display recommendations for this product
    display_recommendations(data['asin'])

    # Assuming this consumer does this in a loop or triggered manner
    # For demonstration, let's show recommendations for a sample ASIN
    if 'sample_asin' in data['asin']:
        display_recommendations('sample_asin')
