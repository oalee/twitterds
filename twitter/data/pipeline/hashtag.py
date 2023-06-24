import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, month, to_date, year, explode, dayofmonth

users_path = os.path.join("/run/media/al/data/td", "users")


spark = (
    SparkSession.builder.appName("Twitter Data Analysis")
    .config(
        "spark.driver.memory", "32g"
    )  # Memory for driver (where SparkContext is initiated)
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.memory", "48g")  # Memory for executor (where tasks are run)
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .config(
        "spark.sql.hive.filesourcePartitionFileCacheSize", 2024 * 1024 * 1024
    )  # 2 GB cache size for partition metadata
    .config("spark.driver.maxResultSize", 2048 * 1024 * 1024)
    .getOrCreate()
)
parquet_files = os.listdir(users_path)
parquet_files = [
    os.path.join(users_path, f, "tweets.parquet")
    for f in parquet_files
    if os.path.exists(os.path.join(users_path, f, "tweets.parquet"))
]


parquet_df = spark.read.parquet(*parquet_files).select(
    "date",
    "hashtags",
)

#  col("hashtags").cast(ArrayType(StringType))
parquet_df = parquet_df.withColumn("hashtags", col("hashtags").cast("array<string>"))

parquet_df = parquet_df.na.drop(subset=["hashtags", "date"])

# aslo drop not a number


# explode the hashtags array
# parquet_df = parquet_df.withColumn("hashtag", explode(col("hashtags")))
# Handle nulls in the "hashtags" column and explode the array
parquet_df = parquet_df.na.drop(subset=["hashtags"])
parquet_df = parquet_df.withColumn("hashtag", explode(col("hashtags")))

# drop na dates
# parquet_df = parquet_df.na.drop(subset=["date"])
# only after 2022
parquet_df = parquet_df.filter(col("date") > "2022-01-01")

# Group by month, day, and hashtag and count
hashtag_by_day_df = (
    parquet_df.groupBy(
        year(col("date")).alias("year"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        "hashtag",
    )
    .count()
    .orderBy("month", "day", "count", ascending=False)
)


# to pandas and save
hashtag_by_day_df.toPandas().to_parquet(
    os.path.join(env["plots"], "analysis", "hashtag.parquet")
)

# Filter for counts more than 10k
trending_hashtags_df = hashtag_by_day_df.filter(col("count") > 10000)

# Show the top 20 trending hashtags
visual_df = trending_hashtags_df.toPandas()
visual_df = visual_df.head(20)

ipdb.set_trace()
# Plot the trending hashtags with matplotlib
visual_df.plot.bar(x="hashtag", y="count")
