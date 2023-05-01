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
import vaex
import ipdb
from concurrent.futures import ThreadPoolExecutor

import fastparquet as fp
import pyarrow as pa

env = Environment()


# Replace filepath with the path to the pickle file
p_path = os.path.join(env["data"], "time", "processed_users.pkl")


# def populate_vx_map():
#     # map from month_year to vaex df
#     # from 2007-01 to 2023-04
#     vx_map = {}
#     for year in range(2007, 2024):
#         for month in range(1, 13):
#             month_year = f"{year}-{month:02}"
#             filepath = os.path.join(
#                 env["data"], "time", f"{month_year}.parquet")
#             if os.path.exists(filepath):
#                 vx_map[month_year] = vaex.open(filepath)
#             else:
#                 vx_map[month_year] = None

#     return vx_map

# vx_map = populate_vx_map()


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
    if value is None:
        return None
    try:
        return np.str0(np.int64(value))
    except (TypeError, ValueError):
        return None


def id_prepro(user_df):
    if user_df.empty:
        return user_df

    user_df['month_year'] = user_df['date'].dt.strftime('%Y-%m')
    # user_df['text'] = user_df['rawContent'].apply(clean_content)

    user_df = user_df[user_df['user'].notnull()].copy()

    # user_df['userId'] = user_df['user'].apply(
    #     lambda x: int(x['id']) if x.get('id') is not None else None)

    remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates', 'text', 'url',
                      'source', 'sourceUrl', 'sourceLabel', 'renderedContent', 'links', 'textLinks']
    user_df = user_df.drop(columns=remove_columns, errors='ignore')

    user_df['media'] = user_df['media'].notnull()

    user_df['userId'] = user_df['user'].apply(
        lambda x: safe_int(x['id']) if x.get('id') is not None else None)

    def get_id(x):
        return safe_int(x['id']) if pd.notna(x) and x.get('id') is not None else None

    user_df['retweetedTweetId'] = user_df['retweetedTweet'].apply(get_id)
    user_df['retweetedUserId'] = user_df['retweetedTweet'].apply(lambda x: safe_int(x['user']['id']) if pd.notna(
        x) and x.get('user') is not None and x['user'].get('id') is not None else None)
    user_df['quotedTweetId'] = user_df['quotedTweet'].apply(get_id)
    user_df['inReplyToUserId'] = user_df['inReplyToUser'].apply(get_id)

    # user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(lambda x: [safe_int(user['id']) for user in x] if pd.notnull(x) else None)

    user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
        lambda x: json.dumps([user['id'] for user in x]) if x is not None else None)

    user_df['hashtags'] = user_df['hashtags'].apply(
        lambda x: json.dumps([item for item in x]) if x is not None else None)
    # json dump hash

    # convert all ids to strings
    for col in ['userId', 'retweetedTweetId', 'conversationId', 'retweetedUserId', 'quotedTweetId', 'inReplyToUserId', 'inReplyToUserId', 'inReplyToTweetId']:
        user_df[col] = user_df[col].apply(
            lambda x: str(x) if pd.notna(x) else None)

    # convert counts to int
    columns = ['replyCount', 'retweetCount',
               'likeCount', 'quoteCount', 'viewCount']

    for col in columns:
        user_df[col] = user_df[col].apply(safe_int)

    return user_df


def append_to_parquet_file(input_file, new_data):

    # if new_data.:
    #     return
    if os.path.exists(input_file):
        try:

            old_data = vaex.read_parquet(input_file)
            new_data = vaex.concat([old_data, new_data], ignore_index=True)
            # drop duplicates
            # new_data = new_data.drop_duplicates( subset=['id'], keep='last')
        except:
            print("Error reading parquet file", input_file)

    # if new_data.empty:

            # return

    new_data.export(input_file, index=False)


def save_data_to_parquet(grouped_data, prefix):
    # global vx_map
    print("Saving data to parquet")
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

        print("Saved data to parquet", output_path)
        # if vx_map[month_year] is not None:
        #     vx_map[month_year] = vx_map[month_year].concat(group)
        # else:
        #     vx_map[month_year] = group
        # export to parquet
        # ipdb.set_trace()
        # vx_map[month_year].export(output_path, index=False)
        # print("Saved data to parquet", output_path)
        # append_to_parquet_file(output_path, group)


def process_batch(tweets_batch, retweets_batch):

    tweets_df = pd.concat([t for t in tweets_batch])
    # retweets_df = pd.concat([r for r in retweets_batch])
    # retweets_df = vaex.concat([vaex.from_pandas(r) for r in retweets_batch])

    group_tweets = tweets_df.groupby('month_year')
    # rt_group_tweets = retweets_df.groupby('month_year')

    # group_retweets = retweets_df.groupby('month_year')

    # def wrapper(groups, prefixs):
    #     for group, prefix in zip(groups, prefixs):
    #         save_data_to_parquet(group, prefix)

    # io_thread = threading.Thread(
    #     target=wrapper, args=([group_tweets, group_retweets], ['tweets', 'retweets']))
    # io_thread.start()

    # with ThreadPoolExecutor() as executor:
    #     executor.submit(save_data_to_parquet, group_tweets)

    # io_th = threading.Thread(target=save_data_to_parquet, args=(group_tweets,))
    # io_th.start()
    save_data_to_parquet(group_tweets, "tweets")
    # save_data_to_parquet(rt_group_tweets, "retweets")
    # wrapper([group_tweets], ['tweets'])

    # io_thread = threading.Thread(target=export_vx_map)


def process_user(username):
    user_df = get_user(username)

    if user_df is None or user_df.empty:
        return None, None

    user_df = id_prepro(user_df)  # preprocess tweets and add ids
    user_df = user_df.drop(
        columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers', 'user'])

    # ipdb.set_trace()
    retweets = user_df[user_df['retweetedTweetId'].notnull()]
    tweets = user_df[user_df['retweetedTweetId'].isnull()]

    return tweets, retweets


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
        tweets, retweets = process_user(username)
        if tweets is not None or retweets is not None:
            # Accumulate tweets and retweets in their respective lists
            tweets_batch.append(tweets)
            retweets_batch.append(retweets)

        if (idx + 1) % batch_size == 0:
            process_batch(tweets_batch, retweets_batch)
            tweets_batch = []
            retweets_batch = []
            save_processed_users(processed_users)

        processed_users.add(username)

    # Process the remaining data
    if tweets_batch or retweets_batch:
        process_batch(tweets_batch, retweets_batch)


if __name__ == "__main__":
    extract()
