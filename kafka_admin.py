from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic

bootstrap_servers = 'localhost:9092'
topic_name = '265225'

admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Delete topic

def delete_topics(topics):
    admin_client.delete_topics(topics=topics)

def get_all_topics():
    topics=admin_client.list_topics()
    topic_names = [topic for topic in topics]
    print(len(topic_names))
    return topic_names



# delete_topics(['test'])
# get_all_topics()
