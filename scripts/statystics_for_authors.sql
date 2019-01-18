
-- średnie wszystkiego bez podziału na autorów
select avg(followers_count), stddev_samp(followers_count), min(followers_count), max(followers_count) from author;
select avg(statuses_count), stddev_samp(statuses_count), min(statuses_count), max(statuses_count) from author;
select avg(friends_count), stddev_samp(friends_count), min(friends_count), max(friends_count) from author;
select avg(retweet_count), stddev_samp(retweet_count), min(retweet_count), max(retweet_count) from tweet;
select avg(favorite_count), stddev_samp(favorite_count), min(favorite_count), max(favorite_count) from tweet;
select avg(array_length(regexp_split_to_array(tweet.content, ' '), 1)) as tweet_len from author inner join tweet on author.id::bigint = tweet.author_id::bigint;

-- średnie per autor

-- srednia ilość znaków w tweet per author
select avg(array_length(regexp_split_to_array(tweet.content, ' '), 1)) as tweet_len, author_id from tweet group by author_id order by tweet_len desc

-- srednia ilość rt tweeta per author
select avg(retweet_count) as tweet_rt, author_id from tweet group by author_id order by tweet_rt desc

-- srednia polubien twittow per author
select avg(favorite_count) as tweet_fav, author_id from tweet group by author_id order by tweet_fav desc

-- statystyki dla autorów wszystkich

-- srednia, stddev, min, max RT twittow autorow
select avg(r.tweet_rt), stddev_samp(r.tweet_rt), min(r.tweet_rt), max(r.tweet_rt) from (select avg(retweet_count) as tweet_rt from tweet group by author_id) as r;

-- srednia, stddev, min, max polubien twittow autorow
select avg(r.tweet_fav), stddev_samp(r.tweet_fav), min(r.tweet_fav), max(r.tweet_fav) from (select avg(favorite_count) as tweet_fav from tweet group by author_id) as r;

-- srednia ilość znaków w tweet autorow
select avg(r.tweet_len), stddev_samp(r.tweet_len), min(r.tweet_len), max(r.tweet_len) from (select avg(array_length(regexp_split_to_array(tweet.content, ' '), 1)) as tweet_len from tweet group by author_id) as r; 



select count(id) as ct, author_id from tweet group by author_id order by ct desc