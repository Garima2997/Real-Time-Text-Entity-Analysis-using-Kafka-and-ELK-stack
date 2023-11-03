import requests
import time
from kafka import KafkaProducer
import json

def serializer(message):
    return json.dumps(message).encode('utf-8')
    
# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

# Continuously fetch news articles and extract named entities
if __name__ == "__main__":
while True:
    allArticles = []
    response = requests.get('https://newsapi.org/v2/everything?q=apple&apiKey=d872e14274fc41159a6de718fa4ca802').json()
    articles = response.get('articles', [])
    for a in articles:
        title = a.get('title', '')
        allArticles.append(title)
    # Wait for 15 minutes
    producer.send('Topic1', ' '.join(allArticles))
    time.sleep(900) 