# Reporting-Tool-with-Kafka

It is Simple Reporting Tool (SRT) for the Twitter data.
The tool provide some interesting statistics and summaries based on tweets dataset. As the underlying temporary storage, Kafka is used.
 
SRT consists of the following pieces:
1.	Kafka producer which simulates using Twitter API by reading the tweets from the existing dataset and posts them to Kafka.
2.	Multi-broker Kafka installation
3.	An application containing Kafka consumer which reads the twitter data from Kafka and builds the reports.

Reports answer the following questions:
1)	The list of all the accounts that you have tweets from
2)	The last 10 tweets for each of the 10 accounts which posted the highest number of tweets in the last 3 hours.
3)	The aggregated statistics with a number of tweets per each hour for all the accounts (for the last 3 hours).
4)	Top 20 most “tweet-producing” accounts for the last n hours (n should be given as a param).
5)	the most popular hashtags across all the tweets during the last n hours (n should be given as a param)


 Design doc for the system
 <img width="576" alt="Снимок" src="https://user-images.githubusercontent.com/44239963/116027131-b7748180-a65c-11eb-840b-6ab23a5d3b29.PNG">




To download results of report:

```gsutil -m cp \
  "gs://ucuc_big_data_hw2_bucket/hw2_reports/report1.json" \
  "gs://ucuc_big_data_hw2_bucket/hw2_reports/report2.json" \
  "gs://ucuc_big_data_hw2_bucket/hw2_reports/report3.json" \
  "gs://ucuc_big_data_hw2_bucket/hw2_reports/report4.json" \
  "gs://ucuc_big_data_hw2_bucket/hw2_reports/report5.json" \
 ``` 
 or http links
 ```
 https://storage.cloud.google.com/ucuc_big_data_hw2_bucket/hw2_reports/report1.json
 https://storage.cloud.google.com/ucuc_big_data_hw2_bucket/hw2_reports/report2.json
 https://storage.cloud.google.com/ucuc_big_data_hw2_bucket/hw2_reports/report3.json
 https://storage.cloud.google.com/ucuc_big_data_hw2_bucket/hw2_reports/report4.json
 https://storage.cloud.google.com/ucuc_big_data_hw2_bucket/hw2_reports/report5.json
 ```
 .
