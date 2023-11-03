import requests
import nltk
import time
import re
import json

from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.corpus import stopwords
from kafka import KafkaProducer
from kafka import KafkaConsumer
from functools import reduce
from collections import Counter

nltk.download('all')

# Continuously extract named entities
if __name__ == "__main__":
while True:
    for message in consumer:
        articles = json.loads(message.value)
        namedEntities = namedEntities(articles)
        wordsCount = Counter(namedEntities)
        producer.send('userCount', wordsCount.most_common(10)
        
def serializer(message):
    return json.dumps(message).encode('utf-8')
    
# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

# Kafka Consumer 
consumer = KafkaConsumer(
    'Topic1',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest'
)

    
#extract named entities from text
def namedEntities(text):
    entities = []
    text = re.sub(r'[^\w\s]', '', str(text))    
    tokens = word_tokenize(text)
    stopWords = set(stopwords.words('english'))
    filteredWords = [word for word in tokens if word.lower() not in stopWords]
    tagged = pos_tag(filteredWords)

    for chunk in tagged:
        if chunk[1] == 'NNP':
            entities.append(''.join(chunk[0]))
    return entities
    


    
