import csv
import time
import json
from google.cloud import storage
from google.cloud.storage import blob
from datetime import datetime, timedelta

start_time = None
n = None

def post_reports(dct, storage_link):
    first_report = json.dumps(list(dct.keys()))
    lst = list(dct.items())
    lst.sort(key=lambda x: len(x[1]), reverse=True)
    lst = lst[:10]
    lst = [(key, sorted(value, key=lambda x: x[1], reverse=True)[:10]) for (key, value) in lst]
    second_report = json.dumps(lst)
    third_report = []

    for key, values in dct.items():
        one = 0
        two = 0
        three = 0
        for value in values:
            tweet_id, tweet_time = value
            tweet_time = datetime.strptime(tweet_time, '%Y-%m-%d %H:%M:%S')
            if tweet_time <= start_time + timedelta(hours=1):
                one += 1
                continue
            if tweet_time <= start_time + timedelta(hours=2):
                two += 1
                continue
            three += 1
        third_report.append((key, {"one": one, "two": two, "three": three}))

    third_report = json.dumps(third_report)

    print(first_report)
    print(second_report)
    print(third_report)


if __name__ == '__main__':
    dct = dict()
    n = int(input("Input n: "))
    storage_link = input("Input storage link: ")
    end_time = datetime.now()
    start_time = datetime.now() - timedelta(hours=3)

    with open('output.csv', newline='', encoding="utf8") as csvfile:
        spam_reader = csv.reader(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in spam_reader:
            tweet_id, user_id, tweet_time = row
            tweet_time = tweet_time.split('.')[0]
            tweet_time = datetime.strptime(tweet_time, '%Y-%m-%d %H:%M:%S')
            if tweet_time < start_time:
                continue
            dct[user_id] = dct.get(user_id, []) + [(tweet_id, str(tweet_time))]

    post_reports(dct, storage_link)


