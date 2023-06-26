import os
from ..loader.spark import get_tweets_session, get_spark_session

# in active users have less than 500 tweets

import yerbamate

env = yerbamate.Environment()


save_path = os.path.join(env["save"], "users", "inactive")

 
spark = get_tweets_session("userId")

# group by userId and count the number of tweets
users_distribution = spark.groupBy("userId").count()

# filter users with less than 500 tweets

users_distribution = users_distribution.filter("count < 500")

# save to parquet

save_path = os.path.join(env["save"], "users", "inactive")

if not os.path.exists(save_path):
    os.makedirs(save_path, exist_ok=True)


# save userId and count to parquet
users_distribution.write.parquet(
    save_path,
    mode="overwrite",
)


def get_user_tweet_count_distribution():
    return users_distribution
