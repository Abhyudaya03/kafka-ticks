from kafka import KafkaConsumer, TopicPartition
import json
import kafka_admin

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

topics_set=set()

topics=kafka_admin.get_all_topics()
# topics=['260105']

def messages_to_from_time(from_timestamp, to_timestamp, topic):
    topic_partitions=[]
    for topic in topics:
        if(topic=='__consumer_offsets' or topic=='quickstart-events'):
            continue
        partitions= consumer.partitions_for_topic(topic)
        # print(partitions)
        tp=[TopicPartition(topic, partition) for partition in partitions]
        topic_partitions.extend(tp)
    print(topic_partitions)
    consumer.assign(topic_partitions)
    partition_from_timestamp = {topic_partition: from_timestamp for topic_partition in topic_partitions}
    partition_to_timestamp = {topic_partition: to_timestamp for topic_partition in topic_partitions}
    mapping_from_time=consumer.offsets_for_times(timestamps=partition_from_timestamp)
    mapping_to_time=consumer.offsets_for_times(timestamps=partition_to_timestamp)
    next_offset = consumer.end_offsets(topic_partitions)

    # print((mapping_from_time))
    
    # for ((topic_partition_from, offset_and_timestamp_from), (topic_partition_to, offset_and_timestamp_to)) in zip(mapping_from_time.items(), mapping_to_time.items()):
    #     consumer.seek(topic_partition_from, offset_and_timestamp_from.offset)
    #     print(offset_and_timestamp_from, offset_and_timestamp_to,topic_partition_from,topic_partition_to)
    #     for message in consumer:
    #         topics_set.add(message.topic)
    #         # print(message)
    #         # print(offset_and_timestamp_to.offset)
    #         if(offset_and_timestamp_to==None):
    #             print("none",message,offset_and_timestamp_to)
    #         if(message.offset>=offset_and_timestamp_to.offset-1):
    #             break
    tn=0
    for topic_partition in topic_partitions:
        offset_and_timestamp_from=(mapping_from_time[topic_partition])
        if(offset_and_timestamp_from==None):
            tn+=1
            continue
        # print(topic_partition)
        offset_from=offset_and_timestamp_from.offset
        consumer.seek(topic_partition, offset_from)
        offset_and_timestamp_to=(mapping_to_time[topic_partition])
        if(offset_and_timestamp_to==None):
            offset_to=next_offset[topic_partition]-1
        else:
            offset_to=offset_and_timestamp_to.offset
        for message in consumer:
            print(message.offset,message.topic)
            if(message.offset>=offset_to):
                break
        
        

    # print(len(topics_set))
    # print(tn)
    # print(len(topic_partitions))


# Use the Kafka consumer object
# for message in consumer:
#     print(message.offset,'\n')
#     break


# print(consumer.topics())
# num=0
# for message in consumer:
#     print(num)
#     num+=1
# consumer.close()



messages_to_from_time(1586735414217, 1786735497004, topics)