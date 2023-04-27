# pip3 install kafka-python --user
import kafka

admin_client = kafka.KafkaAdminClient(bootstrap_servers=["127.0.0.1:9092"])
topic_list = admin_client.list_topics()

for topic in topic_list:
    print(topic)

# ⭐️ Delete all topics
# admin_client.delete_topics(topics=topic_list)
# print("topic deleted")

# topic_list = admin_client.list_topics()
# for topic in topic_list:
#     print(topic)

# ⭐️ adding new topic in kafka
# def create_topics(topic_names):

#     existing_topic_list = consumer.topics()
#     print(list(consumer.topics()))
#     topic_list = []
#     for topic in topic_names:
#         if topic not in existing_topic_list:
#             print('Topic : {} added '.format(topic))
#             topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=3))
#         else:
#             print('Topic : {topic} already exist ')