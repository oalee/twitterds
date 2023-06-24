from ..loader.spark import get_tweets_session
import os, yerbamate
from pyspark.sql.functions import col, explode

env = yerbamate.Environment()

columns = ["userId", "hashtags", "date"]

parquet_df = get_tweets_session(columns)

# this is spark dataframe
# drop null hashtags
parquet_df.na.drop(subset=["hashtags"])

# date after 2022
parquet_df = parquet_df.filter(col("date") > "2022-09-01")

parquet_df = parquet_df.select("userId", explode(col("hashtags")).alias("hashtag"))

# drop hashtags column
parquet_df = parquet_df.drop("hashtags")

parquet_df = parquet_df.groupBy("userId", "hashtag").count()

# save parquet, mode overwrite if exists
parquet_df.write.mode("overwrite").parquet(
    os.path.join(env["plots"], "analysis", "user_hashtag.parquet")
)
