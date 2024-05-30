import json
import time
from kafka import KafkaProducer

def get_data():
    for line in open('/home/awais-khaleeq/kafka/Processed_data.json', 'r'):
        data = json.loads(line)
        filtered_data = {
            'also_buy': data.get('also_buy', []),
            'title': data.get('title', ''),
            'asin': data.get('asin', '')  # Added 'asin' for completeness
        }
        yield filtered_data

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for data in get_data():
    producer.send('topic1', value=data)
    producer.send('topic2', value=data)  # Send to both topic1 and topic2
    print('Data sent to Kafka:', data)
    time.sleep(1)

