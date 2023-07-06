from yerbamate import env

from twitter.data.loader.spark import get_tweets_session, spark
import os
from pyspark.sql.functions import col, explode, count, from_json

from pyspark.sql.types import ArrayType, StringType
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

label_path = os.path.join(env["save"], "hashtag_propaganda_labeled.csv")
# read CSV file into DataFrame
propaganda_labels_df = spark.read.csv(label_path, header=True)


# Define the schema for the 'affiliation' column
array_schema = ArrayType(StringType())

# Convert the 'affiliation' column from a JSON string to an array
propaganda_labels_df = propaganda_labels_df.withColumn(
    "affiliation", 
    from_json("affiliation", array_schema)
)
propaganda_labels_df = propaganda_labels_df.select("hashtag", explode(col("affiliation")).alias("affiliation"))

parquet_df = parquet_df.select("userId", explode(col("hashtags")).alias("hashtag"))

parquet_df = parquet_df.drop("hashtags")


joined_df = parquet_df.join(propaganda_labels_df, "hashtag", "inner")

usage_count = joined_df.groupBy('userId', 'affiliation').agg(count('hashtag').alias('count'))

usage_count.write.mode("overwrite").parquet(
    os.path.join(env["save"], "hashtags", f"user_hashtag_affiliation.parquet")
)
