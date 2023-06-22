import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, month, to_date, year

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
    )  # 1 GB cache size for partition metadata
    .getOrCreate()
)
parquet_files = os.listdir(users_path)
parquet_files = [
    os.path.join(users_path, f, "tweets.parquet")
    for f in parquet_files
    if os.path.exists(os.path.join(users_path, f, "tweets.parquet"))
]


parquet_df = spark.read.parquet(*parquet_files).select(
    "user.created",
    "userId",
)

# drop duplicates on userId
parquet_df = parquet_df.dropDuplicates(["userId"])
# alias user.created as created
parquet_df = parquet_df.withColumnRenamed("user.created", "created")


# First, group by year and plot
year_df = (
    parquet_df.groupBy(
        year(col("created")).alias("year"),
    )
    .count()
    .orderBy("year")
)

pandas_df_year = year_df.toPandas()

pandas_df_year.plot(x="year", y="count", kind="bar")
plt.title("Distribution of Users Creation Date by Year")
plt.xlabel("Year")
plt.ylabel("Count")
# plt.show()?


save_path = os.path.join(env["plots"], "analysis", "user_date")

if not os.path.exists(save_path):
    os.makedirs(save_path, exist_ok=True)

# save plot for year
pandas_df_year.plot(x="year", y="count", kind="bar")
# plt.title("Distribution of Users Creation Date by Year")
# plt.xlabel("Year")
# plt.ylabel("Count")
plt.savefig(os.path.join(save_path, "user_date_year.png"))

# Then, filter for users created after 2022, group by year and month, and plot
parquet_df_after_2022 = parquet_df.filter(year(col("created")) > 2021)

year_month_df = (
    parquet_df_after_2022.groupBy(
        year(col("created")).alias("year"),
        month(col("created")).alias("month"),
    )
    .count()
    .orderBy("year", "month")
)

pandas_df_year_month = year_month_df.toPandas()

# To plot data with both year and month, you could concatenate the year and month columns into a single column
pandas_df_year_month["year_month"] = (
    pandas_df_year_month["year"].astype(str)
    + "-"
    + pandas_df_year_month["month"].astype(str).str.zfill(2)
)

pandas_df_year_month.plot(x="year_month", y="count", kind="bar")
plt.title("Distribution of Users Creation Date by Year and Month (after 2022)")
plt.xlabel("Year-Month")
plt.ylabel("Count")
plt.xticks(rotation=45)
# plt.show()

# save plot for year and month
plt.savefig(os.path.join(save_path, "user_date_year_month.png"))
