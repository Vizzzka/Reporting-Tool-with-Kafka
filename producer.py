import time
import datetime
import csv
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        future = producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        record_metadata = future.get(timeout=10)
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
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))

    with open('archive/sample.csv', newline='', encoding="utf8") as csvfile:
        spam_reader = csv.reader(csvfile, delimiter=',', )
        next(spam_reader)

        for row in spam_reader:
            row[3] = str(datetime.datetime.now())
            key = row[0]
            value = ','.join(row[1:])
            print(key)
            print(value)
            publish_message(producer, "tst3", key, value)

            # wait 40sec
            now = time.time()
            future = now + 40
            while time.time() < future:
                pass

    producer.close()