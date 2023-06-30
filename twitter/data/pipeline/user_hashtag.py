from ..loader.spark import get_tweets_session
import os
from pyspark.sql.functions import col, explode

from yerbamate import env

# env = yerbamate.Environment()


period = env.get_hparam("period", "after")
#import ipdb; ipdb.set_trace()
print(period)

if period == "after":
    time_filter = ((col("date") > "2022-09-14") & (col("date") < "2023-04-17"))

elif period == "before":
    time_filter = ((col("date") > "2022-01-01") & (col("date") < "2022-09-14"))
elif period == "all":
    time_filter = ((col("date") > "2022-01-01" )& (col("date") < "2023-04-17"))
else:
    raise ValueError("Invalid period")

columns = ["userId", "hashtags", "date"]

parquet_df = get_tweets_session(columns)

# this is spark dataframe
# drop null hashtags
parquet_df.na.drop(subset=["hashtags"])

# date after 2022
parquet_df = parquet_df.filter(
    time_filter
)
parquet_df = parquet_df.select("userId", explode(col("hashtags")).alias("hashtag"))

# drop hashtags column
parquet_df = parquet_df.drop("hashtags")

parquet_df = parquet_df.groupBy("userId", "hashtag").count()

# save parquet, mode overwrite if exists
parquet_df.write.mode("overwrite").parquet(
    os.path.join(env["plots"], "analysis", f"user_hashtag_{period}.parquet")
)
