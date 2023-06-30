from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import yerbamate, os


from pyspark.sql import SparkSession
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
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.4")
    # 1 GB cache size for partition metadata
    .getOrCreate()
)

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

    # NLP pipeline
    documentAssembler = DocumentAssembler()\
        .setInputCol("rawContent")\
        .setOutputCol("document")

    tokenizer = Tokenizer()\
        .setInputCols(["document"])\
        .setOutputCol("token")

    lemmatizer = LemmatizerModel.pretrained()\
        .setInputCols(["token"])\
        .setOutputCol("lemma")

    ngrams = NGramGenerator()\
        .setInputCols(["lemma"])\
        .setOutputCol("ngrams")\
        .setN(2)\
        .setEnableCumulative(False)\
        .setDelimiter("_")

    pipeline = Pipeline(stages=[
        documentAssembler,
        tokenizer,
        lemmatizer,
        ngrams
    ])

    model = pipeline.fit(parquet_df)
    result = model.transform(parquet_df)

    return result

if __name__ == "__main__":
    # test
    spark = get_tweets_session("userId", "rawContent")
    spark.show()
    import ipdb; ipdb.set_trace()