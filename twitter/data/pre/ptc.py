from concurrent.futures import ThreadPoolExecutor
import pickle
import json
from multiprocessing import Pool, cpu_count
import os
import threading
import pandas as pd
from yerbamate import Environment
from ..loader.prepro import get_userlist, get_user, clean_text
import tqdm
import numpy as np
import dask.dataframe as dd
import ipdb

env = Environment()


# Replace filepath with the path to the pickle file
p_path = os.path.join(env["data"], "time", "processed_users.pkl")


# dask df indexed by month_tweets.parquet and month_retweets.parquet
#

def save_processed_users(processed_users, filepath=p_path):
    with open(filepath, 'wb') as f:
        pickle.dump(processed_users, f)

    # print("Saved processed users to:", filepath)


def load_processed_users(filepath=p_path):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'rb') as f:
        return pickle.load(f)


def clean_content(content):
    if not isinstance(content, str):
        content = str(content)
    return clean_text(content)


def safe_int(value):
    try:
        return str(np.int64(value))
    except (TypeError, ValueError):
        return None


def id_prepro(user_df):
    if user_df.empty:
        return user_df

    user_df['month_year'] = user_df['date'].dt.strftime('%Y-%m')
    user_df['text'] = user_df['rawContent'].apply(clean_content)

    user_df = user_df[user_df['user'].notnull()].copy()

    # user_df['userId'] = user_df['user'].apply(
    #     lambda x: int(x['id']) if x.get('id') is not None else None)

    remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates',
                      'source', 'sourceUrl', 'sourceLabel', 'renderedContent', 'links', 'textLinks']
    user_df = user_df.drop(columns=remove_columns, errors='ignore')

    user_df['media'] = user_df['media'].notnull()

    user_df['userId'] = user_df['user'].apply(
        lambda x: safe_int(x['id']) if x.get('id') is not None else None)

    def get_id(x):
        return safe_int(x['id']) if pd.notna(x) and x.get('id') is not None else None

    user_df['retweetedTweetId'] = user_df['retweetedTweet'].apply(get_id)
    user_df['retweetedUserId'] = user_df['retweetedTweet'].apply(lambda x: int(x['user']['id']) if pd.notna(
        x) and x.get('user') is not None and x['user'].get('id') is not None else None)
    user_df['quotedTweetId'] = user_df['quotedTweet'].apply(get_id)
    user_df['inReplyToUserId'] = user_df['inReplyToUser'].apply(get_id)

    user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
        lambda x: [str(user['id']) for user in x] if x is pd.notna(x) else None)

    # convert all ids to strings
    for col in ['userId', 'retweetedTweetId', 'retweetedUserId', 'quotedTweetId', 'inReplyToUserId', 'inReplyToUserId', 'inReplyToTweetId']:
        user_df[col] = user_df[col].apply(
            lambda x: str(x) if pd.notna(x) else None)

    return user_df


def append_to_parquet_file(input_file, new_data):

    if new_data.empty:
        return
    if os.path.exists(input_file):
        try:

            old_data = pd.read_parquet(input_file)
            new_data = pd.concat([old_data, new_data], ignore_index=True)
        except:
            print("Error reading parquet file", input_file)
            return

    new_data.to_parquet(input_file, index=False, engine='pyarrow')


def save_data_to_parquet(grouped_data, file_prefix):
    for month_year, group in grouped_data:
        output_dir = os.path.join(env['sv_path'], 'time')
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f'{month_year}-{file_prefix}.parquet'
        output_path = os.path.join(output_dir, output_filename)

        append_to_parquet_file(output_path, group)


def process_batch(tweets_batch, retweets_batch):
    tweets_df = pd.concat(tweets_batch, ignore_index=True)
    retweets_df = pd.concat(retweets_batch, ignore_index=True)

    # do this on io thread, so we don't block the main thread

    group_tweets = tweets_df.groupby('month_year')
    group_retweets = retweets_df.groupby('month_year')

    # save_data_to_parquet(group_tweets, 'tweets')
    # save_data_to_parquet(group_retweets, 'retweets')

    def wrapper(groups, prefixs):
        for group, prefix in zip(groups, prefixs):
            save_data_to_parquet(group, prefix)

    # run on main thread
    # wrapper([group_tweets, group_retweets], ['tweets', 'retweets'])

    # run on background thread
    io_thread = threading.Thread(
        target=wrapper, args=([group_tweets, group_retweets], ['tweets', 'retweets']))
    io_thread.start()
    # run on io thre
    

    # run on pool thread
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     executor.submit(wrapper, [group_tweets, group_retweets], ['tweets', 'retweets'])


def process_user(username):
    user_df = get_user(username)

    if user_df is None or user_df.empty:
        return None, None

    user_df = id_prepro(user_df)  # preprocess tweets and add ids
    user_df = user_df.drop(
        columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers', 'user'])

    # Split into tweets and retweets
    retweets = user_df[user_df['retweetedTweetId'].notnull()]
    tweets = user_df[user_df['retweetedTweetId'].isnull()]

    # print("Tweets: ", len(tweets), "Retweets: ", len(retweets))

    return tweets, retweets


def extract():
    users_list = get_userlist()
    processed_users = load_processed_users()
    print("Already ", len(processed_users), "processed users")
    batch_size = 256  # Adjust this value based on your system's memory constraints
    process_size = 8  # Adjust this value based on your system's memory constraints
    unprocessed_users = [
        username for username in users_list if username not in processed_users]

    with Pool(processes=process_size) as pool:
        result_iterator = pool.imap_unordered(process_user, unprocessed_users)

        tweets_batch = []
        retweets_batch = []

        for idx, (tweets, retweets) in enumerate(tqdm.tqdm(result_iterator, total=len(unprocessed_users))):
            if tweets is not None or retweets is not None:
                # Accumulate tweets and retweets in their respective lists
                tweets_batch.append(tweets)
                retweets_batch.append(retweets)

            if (idx + 1) % batch_size == 0:
                process_batch(tweets_batch, retweets_batch)
                tweets_batch = []
                retweets_batch = []
                save_processed_users(processed_users)

            processed_users.add(unprocessed_users[idx])

        # Process the remaining data
        if tweets_batch or retweets_batch:
            process_batch(tweets_batch, retweets_batch)


if __name__ == "__main__":
    extract()
