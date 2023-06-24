import yerbamate, os


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


env = yerbamate.Environment()
users_path = os.path.join(env["data"], "users")


spark = (
    SparkSession.builder.appName("Twitter Data Analysis")
    .config(
        "spark.driver.memory", "48g"
    )  # Memory for driver (where SparkContext is initiated)
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.memory", "52g")  # Memory for executor (where tasks are run)
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", 2024 * 1024 * 1024)
    .config("spark.driver.maxResultSize", 8048 * 1024 * 1024)
    # 1 GB cache size for partition metadata
    .getOrCreate()
)


def get_tweets_session(columns):
    parquet_files = os.listdir(users_path)
    parquet_files = [
        os.path.join(users_path, f, "tweets.parquet")
        for f in parquet_files
        if os.path.exists(os.path.join(users_path, f, "tweets.parquet"))
    ]

    parquet_df = spark.read.parquet(*parquet_files).select(*columns)
    return parquet_df


def get_retweets_session(columns):
    parquet_files = os.listdir(users_path)
    parquet_files = [
        os.path.join(users_path, f, "retweets.parquet")
        for f in parquet_files
        if os.path.exists(os.path.join(users_path, f, "retweets.parquet"))
    ]

    parquet_df = spark.read.parquet(*parquet_files).select(*columns)
    return parquet_df


def get_tweets_and_retweets_session(columns):
    parquet_files = os.listdir(users_path)

    parquet_files = [
        os.path.join(users_path, f, "tweets.parquet")
        for f in parquet_files
        if os.path.exists(os.path.join(users_path, f, "tweets.parquet"))
    ]

    # also retweets
    parquet_files.extend(
        [
            os.path.join(users_path, f, "retweets.parquet")
            for f in parquet_files
            if os.path.exists(os.path.join(users_path, f, "retweets.parquet"))
        ]
    )

    parquet_df = spark.read.parquet(*parquet_files).select(*columns)
    return parquet_df
