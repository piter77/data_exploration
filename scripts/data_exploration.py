import tweepy
import csv
import pandas as pd
import psycopg2

consumer_key = 'KfMKNmydahwQSubsT297DpbYt'
consumer_secret = 'ccCdXjEEM1vPsuA4sgJNAOTwW9EueZHejhBe42VjO2brWexLdt'
access_token = '819912293521772548-hE5XFuCPGIh5K6GWKp541zwf06YJ4sg'
access_token_secret = 'RQahNZKvkS5iVFr5bKF1PI9HpW6yxmhWyPI5weGKU5yaR'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

conn = psycopg2.connect("dbname=postgres user=postgres password=postgres", host="localhost")
cur = conn.cursor()

tags = ["#pis", "#wybory", "#WyborySamorzadowe", "#PlatformaObywatelska", "#wybory2018", "#pls", "#nowoczesna"]\

insert_tag_sql = "INSERT INTO hashtag (phrase) VALUES (%s) RETURNING id"
insert_retweet_sql = "INSERT INTO re_tweet (author_id, tweet_id, date) VALUES (%s, %s, %s)"
insert_user_sql = "INSERT INTO users (id, name) VALUES (%s, %s)"
insert_tweet_sql = "INSERT INTO tweet (id, author_id, hashtag_id, date, content) VALUES (%s, %s, %s, %s, %s)"


for tag in tags:
    csvFile = open(tag+ '.csv', 'a')
    csvWriter = csv.writer(csvFile)

    cur.execute(insert_tag_sql, (tag,))
    conn.commit()
    tag_id = cur.fetchone()[0]
    print(tag_id)

    for tweet in tweepy.Cursor(api.search,q=tag,
                           lang="pl",
                           since="2018-10-10").items():

        if hasattr(tweet, 'retweeted_status') or "RT @" in tweet.text.encode('utf-8'):
            cur.execute(insert_retweet_sql, (tweet.author.id, tweet.id, tweet.created_at))
            conn.commit()
        else:
            cur.execute(insert_tweet_sql, (tweet.id, tweet.author.id, tag_id,  tweet.created_at, tweet.text.encode('utf-8')))
            cur.execute(insert_user_sql, (tweet.author.id, tweet.created_at))
            conn.commit()

        csvWriter.writerow([tweet.created_at, tweet.text.encode('utf-8')])


