import time
import json
from collections import defaultdict
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime, timedelta
import csv
from kafka import KafkaConsumer
from multiprocessing import Process, Manager



def consumeAndStoreData(topic, consumer_group, start_time, end_time, dct):
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['34.122.5.157:9092', '34.123.106.50:9092', '104.197.156.75:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=consumer_group)
    except:
         print("Error while consuming!!")

    for msg in consumer:
        value = msg.value.decode('utf-8')
        if len(value.split(',')) < 5:
            continue
        tweet_id, author_id, _, tweet_time, tweet_text = value.split(',')[:5]
        tweet_time = tweet_time.split('.')[0]
        try:
            tweet_time = datetime.strptime(tweet_time, '%Y-%m-%d %H:%M:%S')
        except:
            continue

        if tweet_time < start_time:
            continue
        if tweet_time > end_time:
            break
        dct[author_id] = dct.get(author_id, []) + [(tweet_id, str(tweet_time), tweet_text)]
        #print(dct[author_id])

    print("quit consumer")


def post_first_report(dct, storage_path):
    first_report = json.dumps(list(dct.keys()))
    print(first_report)
    # Then do other things...
    blob = bucket.get_blob(storage_path)
    blob.upload_from_string(first_report, content_type='application/json')


def post_second_report(dct, storage_path):
    lst = list(dct.items())
    lst.sort(key=lambda x: len(x[1]), reverse=True)
    lst = lst[:10]
    lst = [(key, sorted(value, key=lambda x: x[1], reverse=True)[:10]) for (key, value) in lst]
    second_report = json.dumps(lst)
    print(second_report)
    blob = bucket.get_blob(storage_path)
    blob.upload_from_string(second_report, content_type='application/json')


def post_third_report(dct, start_time, storage_path):
    third_report = []

    for key, values in dct.items():
        tweets_per_hour = {"one": 0, "two": 0, "three": 0}
        for value in values:
            tweet_id, tweet_time, tweet_text = value
            tweet_time = datetime.strptime(tweet_time, '%Y-%m-%d %H:%M:%S')
            if tweet_time <= start_time + timedelta(hours=1):
                tweets_per_hour["one"] += 1
                continue
            if tweet_time <= start_time + timedelta(hours=2):
                tweets_per_hour["two"] += 1
                continue
            tweets_per_hour["three"] += 1
        third_report.append((key, tweets_per_hour))

    third_report = json.dumps(third_report)
    print(third_report)
    blob = bucket.get_blob(storage_path)
    blob.upload_from_string(third_report, content_type='application/json')


def post_fourth_report(dct, storage_path):
    lst = list(dct.items())
    lst.sort(key=lambda x: len(x[1]), reverse=True)
    lst = lst[:20]
    lst = [key for (key, value) in lst]
    fourth_report = json.dumps(lst)
    print(fourth_report)
    blob = bucket.get_blob(storage_path)
    blob.upload_from_string(fourth_report, content_type='application/json')


def post_fifth_report(dct, storage_path):
    hashtag_dct = dict()
    for key, values in dct.items():
        for value in values:
            tweet_text = value[2]
            hashtags = [hashtag.split(" ")[0] for hashtag in tweet_text.split("#")[1:]]
            for hashtag in hashtags:
                hashtag_dct[hashtag] = hashtag_dct.get(hashtag, 0) + 1


    fifth_report = json.dumps(hashtag_dct)
    print(fifth_report)
    blob = bucket.get_blob(storage_path)
    blob.upload_from_string(fifth_report, content_type='application/json')


if __name__ == '__main__':
    n = int(input("Input n: "))
    storage_link = input("Input storage link: ")

    credentials = service_account.Credentials.from_service_account_file("./homew2-311513-905adcd0fdd3.json")
    client = storage.Client(credentials=credentials)
    # https://console.cloud.google.com/storage/browser/[bucket-id]/
    bucket = client.get_bucket(storage_link)


    print("google storage connected....")
    end_time = datetime.now()
    start_time_group3 = datetime.now() - timedelta(hours=3)
    start_time_group_n = datetime.now() - timedelta(hours=n)

    consumers_group3 = [None] * 5
    consume_threads_group3 = [None] * 5

    consumers_group_n = [None] * 5
    consume_threads_group_n = [None] * 5

    with Manager() as manager:
        dct = manager.dict()
        dct2 = manager.dict()
        for i in range(5):
            consume_threads_group3[i] = Process(target=consumeAndStoreData, args=("test", '3hhlkklh',
                                                                      start_time_group3, end_time, dct))
            consume_threads_group_n[i] = Process(target=consumeAndStoreData, args=("test", 'nlhklkhh',
                                                                start_time_group_n, end_time, dct2))

        for i in range(5):
            consume_threads_group3[i].start()
            consume_threads_group_n[i].start()

        print("threads are running")

        for i in range(5):
            consume_threads_group3[i].join()
            consume_threads_group_n[i].join()

        print(len(dct))
        post_first_report(dct, "hw2_reports/report1.json")
        post_second_report(dct, "hw2_reports/report2.json")
        post_third_report(dct, start_time_group3, "hw2_reports/report3.json")

        post_fourth_report(dct2, "hw2_reports/report4.json")
        post_fifth_report(dct2, "hw2_reports/report5.json")