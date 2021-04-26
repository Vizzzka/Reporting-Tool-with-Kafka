import time
from datetime import datetime, timedelta
import csv
import json
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, value):
    try:
        value_bytes = bytes(value, encoding='utf-8')
        future = producer_instance.send(topic_name, value_bytes)
        producer_instance.flush()
        record_metadata = future.get()
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


if __name__ == '__main__':
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['34.122.5.157:9092',
                                                    '34.123.106.50:9092',
                                                    '104.197.156.75:9092'], request_timeout_ms = 600)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))

    with open('archive/twcs/twcs.csv', newline='', encoding="utf8") as csvfile:
        spam_reader = csv.reader(csvfile, delimiter=',', )
        next(spam_reader)

        for row in spam_reader:
            row[3] = str(datetime.now())
            value = ','.join(row)
            publish_message(producer, "test", value)

            # wait 40sec
            now = time.time()
            future = now + 40
            while time.time() < future:
                pass

    producer.close()