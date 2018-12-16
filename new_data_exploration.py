import psycopg2
import matplotlib.pyplot as plt

conn = psycopg2.connect("dbname=postgres user=postgres password=postgres", host="localhost")
cur = conn.cursor()

# TUTAJ FUNKCJE DOSTEPNE: AVG, MIN, MAX, STDDEV
# PER AUTHORS
followers_avg_sql = "select avg(followers_count) from author;"
cur.execute(followers_avg_sql)
conn.commit()
followers_avg = cur.fetchone()[0]

statuses_avg_sql = "select avg(statuses_count) from author;"
cur.execute(statuses_avg_sql)
conn.commit()
statuses_avg = cur.fetchone()[0]

friends_avg_sql = "select avg(friends_count) from author;"
cur.execute(friends_avg_sql)
conn.commit()
friends_avg = cur.fetchone()[0]

# PER TWEETS
retweet_avg_sql = "select avg(retweet_count) from tweet;"
cur.execute(retweet_avg_sql)
conn.commit()
retweet_avg = cur.fetchone()[0]

favorite_avg_sql = "select avg(favorite_count) from tweet;"
cur.execute(favorite_avg_sql)
conn.commit()
favorite_avg = cur.fetchone()[0]

# narazie skipuje to bo tutaj trzeba dodac content w groupby i to mi psuje wyniki, musze to przemyslec
# tweet_length_avg_sql = "select avg(array_length(regexp_split_to_array(tweet.content, ' '), 1)) as tweet_len from author inner join tweet on author.id::bigint = tweet.author_id::bigint;"

# JEST PROBLEM OGOLNIE BO MAMY TERAZ TAK, `E TWEETS ZAWIERA ZALEDWIE PO JEDNYM TWEETCIE DANEGO AUTORA, NIE MA TAKIEGO AUTHOR'A, KToRY BY TWEETOWAl 2 RAZY XDDD ALBO ROBIe COs zLE
# W KAZDYM RAZIE BARDZIEJ ANALIZUJEMY TWEETY NIZ AUTOROW :D

# TUTAJ TWORZE TEN WEKTOR O KTORYM ONA MOWILA ...
# global_info_sql = "select tweet.author_id, author.followers_count, author.friends_count, tweet.retweet_count, tweet.favorite_count, tweet.source_device, array_length(regexp_split_to_array(tweet.content, ' '), 1) as tweet_len from author inner join tweet on author.id::bigint = tweet.author_id::bigint group by tweet.author_id, tweet.content, tweet.source_device, tweet.retweet_count, tweet.favorite_count, author.followers_count, author.friends_count;"
global_info_sql = "select author.id, author.followers_count, author.friends_count, author.statuses_count, avg(tweet.retweet_count) as avg_rewteet, avg(tweet.favorite_count) as avg_favourite, author.verified from author inner join tweet on author.id::bigint = tweet.author_id::bigint group by author.id;"

# sql = "select favorite_count from tweet;"

cur.execute(global_info_sql)
conn.commit()

all = cur.fetchall()  # first of all

# print(all)

authors_with_more_than_avg_followers = filter(lambda x: x[1] >= followers_avg, all)
authors_with_more_than_avg_friends = filter(lambda x: x[2] >= friends_avg, all)
authors_with_more_than_avg_statuses = filter(lambda x: x[3] >= statuses_avg, all)
authors_with_more_than_avg_retweets = filter(lambda x: x[4] >= retweet_avg, all)
authors_with_more_than_avg_favorites = filter(lambda x: x[5] >= favorite_avg, all)
authors_verified = filter(lambda x: x[6], all)

# vvv followers & friends vvv
print(len(set(authors_with_more_than_avg_followers).intersection(set(authors_with_more_than_avg_friends))))

# plt.plot(all)
# plt.show()
