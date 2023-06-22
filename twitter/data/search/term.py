import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType


time_path = os.path.join("/run/media/al/data/td", "users")


# Initialize Spark
spark = (
    SparkSession.builder.appName("Parquet Timestamp Conversion")
    .config("spark.driver.memory", "48g")
    .config("spark.executor.memory", "48g")
    .getOrCreate()
)

parquet_files = [
        "iweirrdoo",
        "iran_____azad",
        "fatisaaad",
        "harim10878735",
        "EmsSayar",
        "Existisda66",
        "mohmadslmni",
        "khedri_ako",
        "Farshad_8731",
        "Zahra61080597",
        "Abolfazl_sharr",
        "simasnipper",
        "MNasehiyan",
        "itsnegry",
        "zeinab45031521",
        "Saeed_Aghebat",
        "Mohamma36788196",
        "JavadAh86733988",
        "Mariyan444",
        "ho3in_iran",
]

import pandas   as pd
import numpy    as np

parquet_files = os.listdir(time_path)
parquet_files = [
    os.path.join(time_path, f, "tweets.parquet")
    for f in parquet_files
    if os.path.exists(os.path.join(time_path, f, "tweets.parquet"))
]


# df = pd.read_parquet(parquet_files[0])
# ipdb.set_trace()
# df_sp = spark.createDataFrame(df)

parquet_df = spark.read.parquet(*parquet_files[:2]).select(
    "rawContent",
    "date",
    "likeCount",
    "user",
    'retweetCount',
    'replyCount',
    'quoteCount',
    'conversationId',
    'lang',
    'id',
    'mentionedUsers',
    'quotedTweetId',
    'quotedTweetUserId',
)


# Load the Parquet file
# parquet_df = spark.read.parquet(parquet_files[0]).select(
#     "rawContent",
#     "date",
#     "likeCount",
#     "user",
# )

# ipdb.set_trace()
# .withColumn("likeCount", col("likeCount").cast("double")) .select(
#         "rawContent",
#         "date",
#         "likeCount",
#         # List other columns you want to include here
#     )


def search(save_path, searches=None):
    # searches = [ 'تنها الترناتیو', 'تنها_الترناتیو', 'تنها راه نجات', 'تنها_راه_نجات', 'الترناتیو']
    # searches = [ 'اسی و مصی', 'Qنی', 'مصی اسی' , 'اسی مصی', 'معصومه' ]
    # ors = ["اسماعیلیون_شیاد"]

    # ands = ["اسماعیلیون", "شیاد"]

    # df = parquet_df[parquet_df["rawContent"].rlike("|".join(searches))]
    words = ["اسماعیلیون", "شیاد"]

    # Create a filter condition that ensures rawContent contains all the words
    condition = None
    for word in words:
        if condition is None:
            condition = parquet_df["rawContent"].rlike(word)
        else:
            condition &= parquet_df["rawContent"].rlike(word)

    df = parquet_df.filter(condition)
    # drop duplicates
    df = df.drop_duplicates(subset=["rawContent"])

    super_liked = df[df["likeCount"] > 1000]

    # filter liked by > 1000
    df = df[df["likeCount"] > 100]

    path = os.path.join(env["data"], "search", save_path)

    # save to json
    df.repartition(1).write.json(os.path.join(path, "liked100"), mode="overwrite")
    super_liked.repartition(1).write.json(os.path.join("liked1000"), mode="overwrite")


search(save_path="ESMAEILIYON_SHIAD")
