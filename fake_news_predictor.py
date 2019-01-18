import psycopg2

conn = psycopg2.connect("dbname=postgres user=postgres password=postgres", host="localhost")
cur = conn.cursor()

get_tweets_sql = "SELECT id, content FROM tweet where content like \'%Warszaw%\';"
get_authors_sql = "SELECT * FROM author;"

user_tweets = "SELECT content from tweet where author_id = '%s';"

get_users_that_tweeted_after_creation = "select author.id from tweet inner join author on tweet.author_id::text=author.id::text where tweet.date::date - author.created_at::date < 1;"

global_analysis = "select source_device, verified, author_id, tweet.id, favorite_count, author.followers_count, tweet.date::date - author.created_at::date from tweet inner join author on tweet.author_id::text = author.id::text order by favorite_count desc;"

retweeters_sql = "select re_tweet.tweet_id, re_tweet.author_id from tweet inner join re_tweet on re_tweet.retweeted_tweet_id::text = tweet.id::text;"

who_how_many_times_retweeted = "select count(re_tweet.tweet_id), re_tweet.author_id from tweet inner join re_tweet on re_tweet.retweeted_tweet_id::text = tweet.id::text group by re_tweet.author_id order by count(re_tweet.tweet_id) desc;"

cur.execute(get_tweets_sql)
conn.commit()

all = cur.fetchall()  # first of all
first = all[0]
second = all[1]


def semantic_similarity(text1, text2):
    text1words = set(text1.split(" "))
    text2words = set(text2.split(" "))

    all_words = text1words.union(text2words)

    both_texts_word_counter = 0

    for word in text1words:
        for word2 in text2words:
            if word in all_words and word2 in all_words and word is word2:
                both_texts_word_counter += 1

    return float(both_texts_word_counter) / len(all_words)


similarities = []

for text1 in all:
    for text2 in all:
        if text1 is not text2:
            similarity = semantic_similarity(text1[1], text2[1])
            if similarity > 0.11:
                similarities.append((text1[1], text2[1]))

print(similarities)


def is_fake_account(account):
    value = 1.0

    if account[3] > 30:  # followers
        value *= 0.53

    cur.execute(user_tweets, (account[0],))
    conn.commit()

    contains_hastag = 1
    all = cur.fetchall()
    for tweet in all:
        if "#" in tweet:
            contains_hastag = 0.96

    value *= contains_hastag

    if account[4] > 50:
        value *= 0.01

    return value <= 0.6


cur.execute(get_authors_sql)
conn.commit()

all = cur.fetchall()

fakes = []

for account in all:
    if not is_fake_account(account):
        fakes.append(account[2])

for fake in fakes:
    cur.execute(user_tweets, (fake[0],))
    conn.commit()

    tweets = cur.fetchall()
    print(len(tweets))

# cur.execute(retweeters_sql, ())
# conn.commit()
# retweeters = cur.fetchall()
#
#
# for retweeter in retweeters:
#     print(str(int(retweeter[0])) + "," + str(int(retweeter[1])))
