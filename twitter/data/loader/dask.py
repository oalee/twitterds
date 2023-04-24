import os
import yerbamate
import ipdb, tqdm
import dask.dataframe as dd
import vaex

import re

def remove_retweets(df):
    return df[~df['text'].str.startswith('RT')]

def remove_mentions(text):
    # Remove URLs
    # text = re.sub(r'http\S+|https\S+', '', text)

    return text.sub(r'(@\w+\s?)', '', text)


env = yerbamate.Environment()
data_path = os.path.join(env["data"], "users")

# Get the list of all Parquet files
parquet_files = []
for user in tqdm.tqdm(os.listdir(data_path)):
    user_folder = os.path.join(data_path, user)
    tweets_parquet_path = os.path.join(user_folder, "tweets.parquet")
    rt_parquet_path = os.path.join(user_folder, "retweets.parquet")

    if os.path.exists(tweets_parquet_path):
        if os.stat(tweets_parquet_path).st_size != 0:
            parquet_files.append(tweets_parquet_path)

    if os.path.exists(rt_parquet_path):
        if os.stat(rt_parquet_path).st_size != 0:
            parquet_files.append(rt_parquet_path)

    if len(parquet_files) > 10000:
        break

    # for file in os.listdir(user_folder):

    #     if file == "tweets.parquet":#file.endswith(".parquet"):
    #         # if not empty
    #         if os.stat(os.path.join(user_folder, file)).st_size != 0:
    #             # only tweets
    #             if file == "tweets.parquet":
    #                 parquet_files.append(os.path.join(user_folder, file))

ipdb.set_trace()


# Read the Parquet files using Dask
df = dd.read_parquet(parquet_files, engine='pyarrow')

columns_to_drop = [column for column in df.columns if column != 'rawContent']
df = df.drop(columns_to_drop, axis=1)

tweets_ddf = df.rename(columns={'rawContent': 'text'})

# Remove retweets
unique_tweets_ddf = remove_retweets(tweets_ddf)

# Remove mentions
unique_tweets_ddf['text'] = unique_tweets_ddf['text'].map(remove_mentions, meta=('text', 'object'))

# Remove duplicates
unique_tweets_ddf = unique_tweets_ddf.drop_duplicates(subset=['text'])

# df = vaex.open_many(parquet_files)

ipdb.set_trace()

# Drop duplicates
# unique_tweets_ddf = tweets_ddf.drop_duplicates(subset=['text'])

# Save the unique tweets to a new Parquet file
# output_path = "unique_tweets.parquet"
# unique_tweets_ddf.to_parquet(output_path, engine='pyarrow', write_options={
#                              'compression': 'snappy'})
