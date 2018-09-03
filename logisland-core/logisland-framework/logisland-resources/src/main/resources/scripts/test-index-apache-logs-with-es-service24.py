#!/usr/bin/env python

# Integration test script
# pip install Faker

import threading
import logging
import time
import datetime
import numpy
import random
from faker import Faker
from kafka import KafkaConsumer, KafkaProducer

kafka_broker_list = 'sd-84190:6667,sd-84191:6667,sd-84192:6667,sd-84186:6667'
input_topic = 'integration_test_raw'


class ApacheLogProducer(threading.Thread):
    """Produce random apache log to input topic"""
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers=kafka_broker_list)
        faker = Faker()

        ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]
        timestr = time.strftime("%Y%m%d-%H%M%S")
        otime = datetime.datetime.now()
        response = ["200", "404", "500", "301"]
        verb = ["GET", "POST", "DELETE", "PUT"]
        resources = ["/list", "/wp-content", "/wp-admin", "/explore", "/search/tag/list", "/app/main/posts",
                     "/posts/posts/explore", "/apps/cart.jsp?appID="]

        while True:
            time.sleep(1)
            otime += datetime.timedelta(seconds=random.randint(30, 300))
            ip = faker.ipv4()
            dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
            tz = datetime.datetime.now().strftime('%z')
            vrb = numpy.random.choice(verb, p=[0.6, 0.1, 0.1, 0.2])

            uri = random.choice(resources)
            if uri.find("apps") > 0:
                uri += `random.randint(1000, 10000)`

            resp = numpy.random.choice(response, p=[0.9, 0.04, 0.02, 0.04])
            byt = int(random.gauss(5000, 50))
            referer = faker.uri()
            useragent = numpy.random.choice(ualist, p=[0.5, 0.3, 0.1, 0.05, 0.05])()
            log_line = '%s - - [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (
                ip, dt, tz, vrb, uri, resp, byt, referer, useragent)

            producer.send(input_topic, str(log_line))


class ApacheLogKafkaConsumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=kafka_broker_list,
                                 auto_offset_reset='earliest')
        consumer.subscribe([input_topic])


        # assert plutot que print
        for message in consumer:
            print (message)

class ApacheLogElasticConsumer(threading.Thread):
    daemon = True

    def run(self):
        # utiliser elasticsearch python pour verifier la présence des logs dans elasticsearch



def main():
    threads = [
        ApacheLogProducer(),
        ApacheLogKafkaConsumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()


# assert type(id) is IntType, "id is not an integer: %r" % id



# check if es 2.4 is running
