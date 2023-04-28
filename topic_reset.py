import kafka

# Get list of all available brokers conencted to localhost cluster
cluster_metadata = kafka.cluster.ClusterMetadata(bootstrap_servers=["127.0.0.1:9092"])
brokers = cluster_metadata.brokers()
broker_ids = [broker.nodeId for broker in brokers]
print("\nThe available broker(s):")
print(broker_ids)

# Show all / delete all kafka topics for the cluster
admin_client = kafka.KafkaAdminClient(bootstrap_servers=["127.0.0.1:9092"])
topic_list = admin_client.list_topics()
print("\nL: List All Topics")
print("D: Delete All Topics")
ans = input("\nInput: ")

if ans == "L":
    for topic in topic_list:
        print(topic)
        print(cluster_metadata.topics())
        # partitions = admin_client.describe_topics([topic]).topics[0].partitions
        # print(f"Topic: {topic}, Partitions: {partitions}")

elif ans == "D":
    for topic in topic_list:
        admin_client.delete_topics(topics=[topic])
        print(topic + " topic deleted")

else:
    print("Enter valid character")


# Adding new topic in kafka
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