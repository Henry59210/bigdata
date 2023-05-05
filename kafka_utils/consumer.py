import json

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


def get_message(topic):
    # 创建一个 Kafka 消费者
    consumer = KafkaConsumer(topic, bootstrap_servers=['20.2.129.187:9092'])

    # 从 Kafka 主题中拉取消息
    for message in consumer:
        print(message.value.decode('utf-8'))

# if __name__ == '__main__':
#     get_message('user_idx')
