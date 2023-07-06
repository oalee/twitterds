from ..loader.spark import get_tweets_session
import os
from pyspark.sql.functions import col, explode

from yerbamate import env

# env = yerbamate.Environment()



time_filter = ((col("date") < "2023-04-17"))


columns = ["userId", "hashtags", "date"]

parquet_df = get_tweets_session(columns)

# this is spark dataframe
# drop null hashtags
parquet_df.na.drop(subset=["hashtags"])

# date after 2022
parquet_df = parquet_df.filter(
    time_filter
)
# parquet_df = parquet_df.select("userId", explode(col("hashtags")).alias("hashtag"))

# drop hashtags column
# parquet_df = parquet_df.drop("hashtags")


# save parquet, mode overwrite if exists
parquet_df.write.mode("overwrite").parquet(
    os.path.join(env["save"], "hashtags", f"user_hashtag_granular.parquet")
)
