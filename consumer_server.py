from pykafka.simpleconsumer import OffsetType

import logging

from pykafka import KafkaClient



logging.getLogger("pykafka.broker").setLevel('ERROR') #logging config

client = KafkaClient(hosts="localhost:9092") #kafka consumer client

topic = client.topics["service-calls"] #topic to read from

consumer_messages = topic.get_balanced_consumer(
    consumer_group = b'pykafka-1',
    auto_commit_enable = False,
    zookeeper_connect = 'localhost:2181'
)

#Print out the messages
for ms in consumer_messages:
    if ms is not None:
        print( ms.value.decode('utf-8') )