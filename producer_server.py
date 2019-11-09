import pathlib
import json
import logging
from pykafka import KafkaClient
import time

INPUT_FILE = 'police-department-calls-for-service.json'

logger = logging.getLogger(__name__)


def read_file() -> json:
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    return data


def generate_data() -> None:
    data = read_file()
    for i in data:
        message = dict_to_binary(i)
        producer.produce(message)
        time.sleep(2)


def dict_to_binary(json_dict: dict) -> bytes:
    """
    Encode your json to utf-8
    :param json_dict:
    :return:
    """
    return json.dumps(json_dict).encode('utf-8')

if __name__ == "__main__":
    client = KafkaClient(hosts="localhost:9092")
    topic = client.topics[b'service-calls']
    print("topics", client.topics)
    producer = topic.get_producer()

    generate_data()
