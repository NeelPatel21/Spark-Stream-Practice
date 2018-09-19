import time
import datetime
import numpy
import random
from faker import Faker

from kafka import KafkaProducer

# Constants
TOPIC = 'test'
BROKER_HOST = '192.168.138.130:9092' # kafka broker
MIN_INTERVAL = 0.1  # minimum interval time interval between two sequential logs in seconds
MAX_INTERVAL = 5  # minimum interval time interval between two sequential logs in seconds

# log generator configuration
resources=["/list","/wp-content","/wp-admin","/explore","/search/tag/list","/app/main/posts","/posts/posts/explore"]
response=["200","404","500","301"]
verb=["GET","POST","DELETE","PUT"]
faker = Faker()


"""
    producer send error listener
"""
def on_send_error(excp):
    print('ERROR :', excp)


""" 
    generate random log string
"""
def nextLog():
    tz = datetime.datetime.now()
    ip = faker.ipv4()
    vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2])
    uri = random.choice(resources)
    resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])

    return ','.join([str(tz), ip, vrb, uri, resp])


producer = KafkaProducer(bootstrap_servers=[BROKER_HOST])

# infinite loop to produce log continually
while True:
    producer.send(TOPIC, bytes(nextLog(), encoding='utf-8')).add_errback(on_send_error)
    if random.randint(0, 1):
        producer.flush()
    time.sleep(random.uniform(MIN_INTERVAL,MAX_INTERVAL))
