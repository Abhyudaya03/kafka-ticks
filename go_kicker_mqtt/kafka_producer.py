from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps  


class KafkaProducerClass():
    def __init__(self, servers):
        self.bootstrap_servers = servers
        try:
            self.producer= KafkaProducer(bootstrap_servers=self.bootstrap_servers, value_serializer = lambda x:str(x).encode('utf-8'))
        except KafkaError as e:
            print(f"Failed to connect to Kafka broker: {str(e)}")

    # def on_send_success(self, record_metadata):
    #     print(f"Message sent successfully. Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

    # def on_send_error(self, exception):
    #     print(f"Error while sending message: {exception}")
