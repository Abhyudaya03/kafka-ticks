import json
from kafka import KafkaConsumer, TopicPartition
import schedule
import time
from datetime import datetime, timedelta, date
from bucket import upload_cs_file
import kafka_admin

# topics=[2815745]
data_to_push=[]

def my_function():
    topics=kafka_admin.get_all_topics()
    # print((topics))
    consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
    )

    for topic in topics:
        if(topic=='__consumer_offsets' or topic=='quickstart-events' or topic=='calc_data' or topic=='orders'):
            continue
        partitions= consumer.partitions_for_topic(topic)
        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        next_offset = consumer.end_offsets(topic_partitions)
        print(next_offset)
        for topic_partition in topic_partitions:
            if(next_offset[topic_partition]!=0):
                consumer.assign([topic_partition]) 
        consumer.seek_to_beginning()
        # print(next_offset[topic_partitions[0]])
        topic_data=[]
        print(topic)
        # print(next_offset[TopicPartition('calc_data',0)])
        for message in consumer:
            print(message.offset,next_offset[TopicPartition(message.topic,message.partition)])
            print(message)
            if(message.offset>=next_offset[TopicPartition(message.topic,message.partition)]-1):
                break
            data_dict=(json.loads(message.value.decode('utf-8')))
            topic_data.append(data_dict)
        print("out of loop")
        # print(topic_data)
        data_to_push.append({topic:topic_data})
        # print("data_appended")
         # upload_folder("/tmp/kafka-logs", "kafka-testing-bucket", f"kafka_data_{date}")

    print("making file......")
    with open('data_to_flush.json', 'w') as json_file:
        json.dump(data_to_push, json_file)

    # result=upload_cs_file("kafka-testing-bucket",'data_to_push.json',f'ticks_data_{date}')
    # if(result):
    #     kafka_admin.delete_topics(topics)


# def run_at_six_pm():
#     # Get the current time in IST
#     current_time = datetime.now() 
#     print(current_time)
    
#     if current_time.hour == 17 and current_time.minute == 12:
#         my_function(date.today())

# # Schedule the job to run every minute
# schedule.every().minute.do(run_at_six_pm)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

my_function()
