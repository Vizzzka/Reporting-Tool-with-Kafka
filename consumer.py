import time
from datetime import datetime, timedelta
import csv
from kafka import KafkaConsumer
from multiprocessing import Process


def consumeData(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='3-hour-group')
    except:
         print("Error while consuming!!")

    for msg in consumer:
        tweet_time = msg.value.decode("utf-8").split(',')[2]
        tweet_id = msg.key.decode("utf-8")
        user_id = msg.value.decode("utf-8").split(',')[0]

        with open('output.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([tweet_id, user_id, tweet_time])


if __name__ == '__main__':
    consumers = [None] * 30
    consume_threads = [None] * 30

    for i in range(30):
        consume_threads[i] = Process(target=consumeData, args=("tst3",))

    for i in range(30):
        consume_threads[i].start()