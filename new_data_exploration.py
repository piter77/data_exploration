import psycopg2
import matplotlib.pyplot as plt

conn = psycopg2.connect("dbname=postgres user=postgres password=postgres", host="localhost")
cur = conn.cursor()

# TUTAJ FUNKCJE DOSTEPNE: AVG, MIN, MAX, STDDEV
#PER AUTHORS
followers_avg_sql = "select avg(followers_count) from author;"
statuses_avg_sql = "select avg(statuses_count) from author;"
friends_avg_sql = "select avg(friends_count) from author;"

#PER TWEETS
retweet_avg_sql = "select avg(retweet_count) from tweet;"
favorite_avg_sql = "select avg(favorite_count) from tweet;"
tweet_length_avg_sql = "select avg(array_length(regexp_split_to_array(tweet.content, ' '), 1)) as tweet_len from author inner join tweet on author.id::bigint = tweet.author_id::bigint;"

# JEST PROBLEM OGOLNIE BO MAMY TERAZ TAK, `E TWEETS ZAWIERA ZALEDWIE PO JEDNYM TWEETCIE DANEGO AUTORA, NIE MA TAKIEGO AUTHOR'A, KToRY BY TWEETOWAl 2 RAZY XDDD ALBO ROBIe COs zLE
# W KAZDYM RAZIE BARDZIEJ ANALIZUJEMY TWEETY NIZ AUTOROW :D

# TUTAJ TWORZE TEN WEKTOR O KTORYM ONA MOWILA ...
global_info_sql = "select tweet.author_id, author.followers_count, author.friends_count, tweet.retweet_count, tweet.favorite_count, tweet.source_device, array_length(regexp_split_to_array(tweet.content, ' '), 1) as tweet_len from author inner join tweet on author.id::bigint = tweet.author_id::bigint group by tweet.author_id, tweet.content, tweet.source_device, tweet.retweet_count, tweet.favorite_count, author.followers_count, author.friends_count;"

# sql = "select favorite_count from tweet;"

cur.execute(global_info_sql)
conn.commit()

all = cur.fetchall()  # first of all

print(all)

# plt.plot(all)
# plt.show()

