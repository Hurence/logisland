from kafka import KafkaConsumer


consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
KafkaConsumer()
consumer.subscribe("logisland_raw")


for msg in consumer:
    print( msg[6] )