import json
import os
import shutil
from kafka import KafkaConsumer, TopicPartition
import schedule
import time
from datetime import datetime, timedelta, date
from bucket import upload_folder
import kafka_admin

# topics=[2815745]
data_to_push=[]

def my_function(date):
    os.mkdir("tick_data")
    topics=kafka_admin.get_all_topics()
    # print((topics))
    consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='test_id'
    )
    clock=0
    filename=1
    for topic in topics:
        if(topic=='__consumer_offsets' or topic=='quickstart-events' or topic=='calc_data' or topic=='orders'):
            continue
        partitions= consumer.partitions_for_topic(topic)
        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        next_offset = consumer.end_offsets(topic_partitions)
        print(next_offset)
        flag=1
        for topic_partition in topic_partitions:
            if(next_offset[topic_partition]!=0):
                consumer.assign([topic_partition]) 
                print(topic)

        consumer.seek_to_beginning()
        # print(next_offset[topic_partitions[0]])
        topic_data=[]
        # print(next_offset[TopicPartition('calc_data',0)])
        try: 
            for message in consumer:
                # print(message.offset,next_offset[TopicPartition(message.topic,message.partition)])
                # print(message)
                if(message.offset>=next_offset[TopicPartition(message.topic,message.partition)]-1):
                    break
                data_dict=(json.loads(message.value.decode('utf-8')))
                topic_data.append(data_dict)
        except KeyError:
            print(KeyError,"Encountered a topic with no message")
        else:    
            print("out of loop")
            # print(topic_data)
            data_to_push.append({topic:topic_data})
            # print("data_appended")
            # upload_folder("/tmp/kafka-logs", "kafka-testing-bucket", f"kafka_data_{date}")
            clock+=1
        if clock==25:
            print("making file......")
            with open(f'./tick_data/data_to_flush_{filename}.json', 'w') as json_file:
                json.dump(data_to_push, json_file)
            clock=0
            filename+=1
            data_to_push.clear()

    print("uploading")
    result=upload_folder("./tick_data","kafka-testing-bucket",f'tick_data_{date}')
    print(result)
    if(result):
        shutil.rmtree('tick_data')
        # kafka_admin.delete_topics(topics)


def run_at_six_pm():
    # Get the current time in IST
    current_time = datetime.now() 
    print(current_time)
    
    if current_time.hour == 17 and current_time.minute == 13:
        my_function(date.today())

# Schedule the job to run every minute
schedule.every().second.do(run_at_six_pm)

while True:
    schedule.run_pending()
    time.sleep(1)

# my_function()
