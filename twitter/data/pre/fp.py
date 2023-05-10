from concurrent.futures import ThreadPoolExecutor
import pickle
import json
from multiprocessing import Pool, cpu_count
import os
import threading
import pandas as pd
from yerbamate import Environment
from ..loader.prepro import get_cleaned_user, get_userlist, get_user, clean_text, get_cleaned_tweets
import tqdm
import numpy as np
import vaex
import ipdb
from concurrent.futures import ThreadPoolExecutor

import fastparquet as fp
import pyarrow as pa

env = Environment()


# Replace filepath with the path to the pickle file
p_path = os.path.join(env["data"], "time", "processed_users.pkl")

# os mkdirs
os.makedirs(os.path.join(env["data"], "time"), exist_ok=True)


def save_processed_users(processed_users, filepath=p_path):
    with open(filepath, 'wb') as f:
        pickle.dump(processed_users, f)

    # print("Saved processed users to:", filepath)


def load_processed_users(filepath=p_path):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'rb') as f:
        return pickle.load(f)


def save_data_to_parquet(grouped_data, prefix):
    # global vx_map
    # print("Saving data to parquet")
    for month_year, group in grouped_data:
        if group is None:
            continue
        output_dir = os.path.join(env['sv_path'], 'time')
        # os.makedirs(output_dir, exist_ok=True)
        month_year = month_year
        output_filename = f'{month_year}_{prefix}.parquet'
        output_path = os.path.join(output_dir, output_filename)

        append = os.path.exists(output_path)

        fp.write(output_path, group, append=append, object_encoding='utf8')

        # print("Saved data to parquet", output_path)


def process_batch(tweets_batch):

    tweets_df = pd.concat([t for t in tweets_batch])

    group_tweets = tweets_df.groupby('month_year')

    save_data_to_parquet(group_tweets, "tweets")


def extract():
    users_list = get_userlist()
    processed_users = load_processed_users()
    print("Already ", len(processed_users), "processed users")
    batch_size = 64  # Adjust this value based on your system's memory constraints
    process_size = 1  # Adjust this value based on your system's memory constraints
    unprocessed_users = [
        username for username in users_list if username not in processed_users]

    tweets_batch = []
    retweets_batch = []

    # handle the case where the process was interrupted

    for idx, username in tqdm.tqdm(enumerate(unprocessed_users), total=len(unprocessed_users)):
        tweets = get_cleaned_user(username)

        # drop date < 2022.08.01

        if tweets is not None and not tweets.empty:
            # Accumulate tweets and retweets in their respective lists

            # if card column exists, drop it, ipdb.set_trace()
            if 'card' in tweets.columns:
                # error: not proccessed, log username
                print("error: not proccessed, log username", username)
                continue

                # ipdb.set_trace()

            tweets = tweets[tweets['date'] >= '2022-08-01']

            tweets_batch.append(tweets)
            # retweets_batch.append(retweets)

        if (idx + 1) % batch_size == 0:
            process_batch(tweets_batch)
            tweets_batch = []
            # retweets_batch = []
            save_processed_users(processed_users)

        processed_users.add(username)

    # Process the remaining data
    if len(tweets_batch) > 0:
        process_batch(tweets_batch)


if __name__ == "__main__":
    extract()
