import yerbamate, os


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


env = yerbamate.Environment()
users_path = os.path.join(env["data"], "users")


spark = (
    SparkSession.builder.appName("Twitter Data Analysis")
    .config(
        "spark.driver.memory", "38g"
    )  # Memory for driver (where SparkContext is initiated)
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.memory", "42g")  # Memory for executor (where tasks are run)
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", 2024 * 1024 * 1024)
    .config("spark.driver.maxResultSize", 8048 * 1024 * 1024)
    # 1 GB cache size for partition metadata
    .getOrCreate()
)


def get_spark_session():
    return spark


def get_tweets_session(columns, *args, **kwargs):
    if type(columns) != list:
        columns = [columns]
        if len(args) > 0:
            columns.extend(args)

    parquet_files = os.listdir(users_path)
    parquet_files = [
        os.path.join(users_path, f, "tweets.parquet")
        for f in parquet_files
        if os.path.exists(os.path.join(users_path, f, "tweets.parquet"))
    ]

    parquet_df = spark.read.parquet(*parquet_files).select(*columns)
    return parquet_df

def get_active_tweets_session(columns, *args, **kwargs):

    if type(columns) != list:
        columns = [columns]
        if len(args) > 0:
            columns.extend(args)

    # in active users, already filtered by tweets count
    inactive_path  = os.path.join(env["save"], "users", "inactive")
    # read that with spark
    inactive_df = spark.read.parquet(inactive_path).select("userId")



    parquet_files = os.listdir(users_path)
    parquet_files = [
        os.path.join(users_path, f, "tweets.parquet")
        for f in parquet_files
        if os.path.exists(os.path.join(users_path, f, "tweets.parquet"))
    ]

    parquet_df = spark.read.parquet(*parquet_files).select(*columns)
 
    # filter by active users, which are the ones that are not in the inactive_df
    parquet_df = parquet_df.join(inactive_df, on="userId", how="left_anti")
    


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
