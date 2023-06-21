import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType


time_path = os.path.join(env["data"], "new")
# Initialize Spark
spark = (
    SparkSession.builder.appName("Parquet Timestamp Conversion")
    .config("spark.driver.memory", "32g")
    .config("spark.executor.memory", "32g")
    .getOrCreate()
)

parquet_files = os.listdir(time_path)
parquet_files = [f for f in parquet_files if f.endswith(".parquet")]
parquet_files = [os.path.join(time_path, f) for f in parquet_files]
# Load the Parquet file with rawContent, date, likeCount
parquet_df = spark.read.parquet(
    *parquet_files, columns=["rawContent", "date", "likeCount"]
)

# searches = [ 'تنها الترناتیو', 'تنها_الترناتیو', 'تنها راه نجات', 'تنها_راه_نجات', 'الترناتیو']
# searches = [ 'اسی و مصی', 'Qنی', 'مصی اسی' , 'اسی مصی', 'معصومه' ]
searches = ["بنغازی"]

df = parquet_df[parquet_df["rawContent"].rlike("|".join(searches))]

# drop duplicates
df = df.drop_duplicates(subset=["rawContent"])


super_liked = df[df["likeCount"] > 1000]

# filter liked by > 1000
df = df[df["likeCount"] > 100]


path = os.path.join(env["data"], "search", "BENGHAZI")


# save to json
df.write.json(os.path.join(path, "liked100"), mode="overwrite")
super_liked.repartition(1).write.json(os.path.join("liked1000"), mode="overwrite")

# # fil = parquet_df.filter(col("rawContent")rlike('|'.join(searches)))
# ipdb.set_trace()

# # Replace 'timestamp_col' with the name of the column with the incompatible TIMESTAMP type.
# # new_df = parquet_df.withColumn("date", col("date").cast(TimestampType()))

# # Now you can work with the new dataframe 'new_df' having the TIMESTAMP type.
# ipdb.set_trace()


# def search(df, search_term):
#     # ipdb.set_trace()
#     # drop na
#     df = df.dropna(subset=["rawContent"])
#     df = df[df["rawContent"].str.contains(search_term)]
#     # df.to_parquet(os.path.join(time_path, "search.parquet"))
#     return df


# def search_all(search_term):
#     df = spark.read.parquet(*parquet_files)

#     return df


# search_term = env.action

# df = search_all(search_term)
