import json

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError


def push_message(topic, content):
    producer = KafkaProducer(bootstrap_servers=['20.2.240.50:9092'])
    producer.send(topic, value=str(content).encode('utf-8'))
    producer.close()


# if __name__ == '__main__':
    # # Kafka集群地址
    # bootstrap_servers = ['20.2.240.50:9092']
    #
    #
    # # 创建Kafka Producer
    # producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
    #                          api_version=(3, 4))
    #
    # # 发送测试消息
    # future = producer.send('test-topic', b'test-message')
    # record_metadata = future.get(timeout=10)
    #
    # # 打印消息发送结果
    # print("Message sent successfully to topic:", record_metadata.topic)
    # print("Partition:", record_metadata.partition)
    # print("Offset:", record_metadata.offset)
    #
    # # 创建Kafka Consumer
    # consumer = KafkaConsumer('test-topic', bootstrap_servers=bootstrap_servers,
    #                          api_version=(3, 4),
    #                          auto_offset_reset='earliest', enable_auto_commit=True)
    #
    # # 读取测试消息
    # for message in consumer:
    #     print("Message received from topic:", message.topic)
    #     print("Partition:", message.partition)
    #     print("Offset:", message.offset)
    #     print("Message value:", message.value)
    #     break
    #
    # # 关闭Kafka Producer和Consumer
    # producer.close()
    # consumer.close()
