import time
from kafka import KafkaConsumer

topic_name = 'topictest'
consumer = KafkaConsumer(topic_name, bootstrap_servers=['172.18.0.25:9092', '172.18.0.47:9092', '172.18.0.44:9092'])


for message in consumer:
    with open('./kafka_con.log', 'a+') as f:
        f.write('%s TOPIC: %s' % (time.strftime('%Y%m%d%H%M%S', time.gmtime()), message.topic))
        f.write(message.value.decode())

    print('-------kafka consumer------')
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value.decode()))
