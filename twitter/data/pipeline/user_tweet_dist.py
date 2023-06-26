import yerbamate
import os
from ..loader.spark import get_tweets_session

env = yerbamate.Environment()


save_path = os.path.join(env["save"], "users", "tweets_distribution")

spark = get_tweets_session("userId")

# group by userId and count the number of tweets
users_distribution = spark.groupBy("userId").count()

# save to parquet

if not os.path.exists(save_path):
    os.makedirs(save_path, exist_ok=True)


# save userId and count to parquet
users_distribution.write.parquet(
    save_path,
    mode="overwrite",
)
