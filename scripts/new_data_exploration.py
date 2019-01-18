import psycopg2
import matplotlib.pyplot as plt
import csv

conn = psycopg2.connect("dbname=postgres user=postgres password=postgres", host="localhost")
cur = conn.cursor()

# TUTAJ FUNKCJE DOSTEPNE: AVG, MIN, MAX, STDDEV
# PER AUTHORS
followers_avg_sql = "select avg(followers_count) from author;"
followers_min_sql = "select min(followers_count) from author;"
followers_max_sql = "select max(followers_count) from author;"
followers_stddev_sql = "select stddev(followers_count) from author;"

cur.execute(followers_avg_sql)
conn.commit()
followers_avg = cur.fetchone()[0]

cur.execute(followers_min_sql)
conn.commit()
followers_min = cur.fetchone()[0]

cur.execute(followers_max_sql)
conn.commit()
followers_max = cur.fetchone()[0]

cur.execute(followers_stddev_sql)
conn.commit()
followers_stddev = cur.fetchone()[0]

print("FOLLOWERS")
print(followers_avg)
print(followers_min)
print(followers_max)
print(followers_stddev)

statuses_avg_sql = "select avg(statuses_count) from author;"
statuses_min_sql = "select min(statuses_count) from author;"
statuses_max_sql = "select max(statuses_count) from author;"
statuses_stddev_sql = "select stddev(statuses_count) from author;"

cur.execute(statuses_avg_sql)
conn.commit()
statuses_avg = cur.fetchone()[0]

cur.execute(statuses_min_sql)
conn.commit()
statuses_min = cur.fetchone()[0]

cur.execute(statuses_max_sql)
conn.commit()
statuses_max = cur.fetchone()[0]

cur.execute(statuses_stddev_sql)
conn.commit()
statuses_stddev = cur.fetchone()[0]

print("STATUSES")
print(statuses_avg)
print(statuses_min)
print(statuses_max)
print(statuses_stddev)

friends_avg_sql = "select avg(friends_count) from author;"
friends_min_sql = "select min(friends_count) from author;"
friends_max_sql = "select max(friends_count) from author;"
friends_stddev_sql = "select stddev(friends_count) from author;"

cur.execute(friends_avg_sql)
conn.commit()
friends_avg = cur.fetchone()[0]

cur.execute(friends_min_sql)
conn.commit()
friends_min = cur.fetchone()[0]

cur.execute(friends_max_sql)
conn.commit()
friends_max = cur.fetchone()[0]

cur.execute(friends_stddev_sql)
conn.commit()
friends_stddev = cur.fetchone()[0]

print("FRIENDS")
print(friends_avg)
print(friends_min)
print(friends_max)
print(friends_stddev)

# PER TWEETS
retweet_avg_sql = "select avg(retweet_count) from tweet;"
retweet_min_sql = "select min(retweet_count) from tweet;"
retweet_max_sql = "select max(retweet_count) from tweet;"
retweet_stddev_sql = "select stddev(retweet_count) from tweet;"

cur.execute(retweet_avg_sql)
conn.commit()
retweet_avg = cur.fetchone()[0]

cur.execute(retweet_min_sql)
conn.commit()
retweet_min = cur.fetchone()[0]

cur.execute(retweet_max_sql)
conn.commit()
retweet_max = cur.fetchone()[0]

cur.execute(retweet_stddev_sql)
conn.commit()
retweet_stddev = cur.fetchone()[0]

print("RETWEETS")
print(retweet_avg)
print(retweet_min)
print(retweet_max)
print(retweet_stddev)

favorite_avg_sql = "select avg(favorite_count) from tweet;"
favorite_min_sql = "select min(favorite_count) from tweet;"
favorite_max_sql = "select max(favorite_count) from tweet;"
favorite_stddev_sql = "select stddev(favorite_count) from tweet;"

cur.execute(favorite_avg_sql)
conn.commit()
favorite_avg = cur.fetchone()[0]

cur.execute(favorite_min_sql)
conn.commit()
favorite_min = cur.fetchone()[0]

cur.execute(favorite_max_sql)
conn.commit()
favorite_max = cur.fetchone()[0]

cur.execute(favorite_stddev_sql)
conn.commit()
favorite_stddev = cur.fetchone()[0]

print("FAVORITES")
print(favorite_avg)
print(favorite_min)
print(favorite_max)
print(favorite_stddev)

# global_info_sql = "select tweet.author_id, author.followers_count, author.friends_count, tweet.retweet_count, tweet.favorite_count, tweet.source_device, array_length(regexp_split_to_array(tweet.content, ' '), 1) as tweet_len from author inner join tweet on author.id::bigint = tweet.author_id::bigint group by tweet.author_id, tweet.content, tweet.source_device, tweet.retweet_count, tweet.favorite_count, author.followers_count, author.friends_count;"
global_info_sql = "select author.id, author.followers_count, author.friends_count, author.statuses_count, cast(avg(tweet.retweet_count) as decimal(10,2)), cast(avg(tweet.favorite_count) as decimal(10,2)), author.verified from author inner join tweet on author.id::bigint = tweet.author_id::bigint group by author.id;"

# sql = "select favorite_count from tweet;"

cur.execute(global_info_sql)
conn.commit()

all = cur.fetchall()  # first of all

authors_with_more_than_avg_followers = filter(lambda x: x[1] >= followers_avg, all)
authors_with_more_than_avg_friends = filter(lambda x: x[2] >= friends_avg, all)
authors_with_more_than_avg_statuses = filter(lambda x: x[3] >= statuses_avg, all)
authors_with_more_than_avg_retweets = filter(lambda x: x[4] >= retweet_avg, all)
authors_with_more_than_avg_favorites = filter(lambda x: x[5] >= favorite_avg, all)
authors_verified = filter(lambda x: x[6], all)

print(len(authors_with_more_than_avg_followers))
print(len(authors_with_more_than_avg_friends))
print(len(authors_with_more_than_avg_statuses))
print(len(authors_with_more_than_avg_retweets))
print(len(authors_with_more_than_avg_favorites))
print(len(authors_verified))


# vvv followers & friends vvv
print(len(set(authors_with_more_than_avg_followers).intersection(set(authors_with_more_than_avg_friends))))

#save vectors to csv
with open('authors.csv', 'wb') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for each in all:
        spamwriter.writerow(each)


# plt.plot(all)
# plt.show()
