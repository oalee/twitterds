from ..loader.spark import get_tweets_session

import yerbamate

env = yerbamate.Environment()

spark = get_tweets_session("userId", "likesCount", "retweetCount")


# distribution of users tweet count(userId)

users_distribution = spark.groupBy("userId").count()
