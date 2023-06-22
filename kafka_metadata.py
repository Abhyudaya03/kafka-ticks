from kafka.cluster import ClusterMetadata

cluster=ClusterMetadata(
    bootstrap_servers='localhost:9092',
)

print(cluster.brokers())