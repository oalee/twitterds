from .id_clean_prepro import id_prepro
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


# global variable userList, need to check if an item is in the list, so it is faster to use a set
userList = set(get_userlist())
new_users = set()


def save_processed_users(processed_users, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(processed_users, f)

    # print("Saved processed users to:", filepath)


def load_processed_users(filepath):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'rb') as f:
        return pickle.load(f)


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


def process_batch(tweets_batch):

    tweets_df = pd.concat([t for t in tweets_batch])

    group_tweets = tweets_df.groupby('month_year')

    save_data_to_parquet(group_tweets, "tweets")


def process_profile(user_df):

    # get rewtweetedTweetUserNames

    rt_user_names = user_df['retweetedTweet'].apply(
        lambda x: x['user']['username'] if x is not None else None)

    # get quotedTweetUserNames


    # qt_user_names = user_df['quotedTweet'].apply(
    #     lambda x: x['quotedTweet']['user']['username'] if x is not None and 'quotedTweet' in x and x['quotedTweet'] is not None and 'user' in x['quotedTweet'] else None)

    not_none_quoted_tweets = user_df[pd.notnull(
        user_df['quotedTweet'])]['quotedTweet']
    qt_user_names = not_none_quoted_tweets.apply(
        lambda x: x['user']['username'] if 'user' in x and x['user'] is not None else None)

    # get inReplyToUserNames
    ir_user_names = user_df['inReplyToUser'].apply(
        lambda x: x['username'] if x is not None else None)

    # get mentionedUserNames
    mt_user_names = user_df['mentionedUsers'].apply(
        lambda x: [user['username'] for user in x] if x is not None else None)
    

    unique_users = user_df.loc[user_df['user'].apply(
        lambda x: x['id']).drop_duplicates().index, 'user']

    self_user = unique_users.to_list()[0]

    path = os.path.join(
        env['sv_path'], self_user['username'], 'user_self.pickle')

    # save user of user_self to a pickle file,
    # path env['sv_path']/{user_name}/user_self.pickle

    def save_user_to_pickle(user, path):

        with open(path, 'wb') as f:
            pickle.dump(user, f)

    # combine all user names, except self_user
    # all_user_names = pd.concat(
    #     [rt_user_names, qt_user_names, ir_user_names, mt_user_names]).drop_duplicates().to_list()

    flat_mt_users = [user for sublist in mt_user_names for user in (sublist if sublist is not None else [])]

    all_user_names = pd.concat(
        [rt_user_names, qt_user_names, ir_user_names, pd.Series(flat_mt_users)]
    ).drop_duplicates().to_list()
    
    # check if user is in userList
    global userList
    ipdb.set_trace()


    # check if all_user_names is in userList
    # all_user_names = [user for user in all_user_names if user not in userList]



    # get all user names


def process_user(username):
    user_df = get_user(username)

    if user_df is None or user_df.empty:
        return None, None

    user_df = id_prepro(user_df)  # preprocess tweets and add ids

    profiles = process_profile(user_df)

    user_df = user_df.drop(
        columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers', 'user'])

    # ipdb.set_trace()
    retweets = user_df[user_df['retweetedTweetId'].notnull()]
    tweets = user_df[user_df['retweetedTweetId'].isnull()]

    return tweets, retweets


def extract():
    users_list = get_userlist()

    path = os.path.join(env['sv_path'], 'profile_p_users.pickle')

    processed_users = load_processed_users(path)
    print("Already ", len(processed_users), "processed users")
    batch_size = 64  # Adjust this value based on your system's memory constraints
    process_size = 1  # Adjust this value based on your system's memory constraints
    unprocessed_users = [
        username for username in users_list if username not in processed_users]

    tweets_batch = []
    retweets_batch = []

    # handle the case where the process was interrupted

    for idx, username in tqdm.tqdm(enumerate(unprocessed_users), total=len(unprocessed_users)):

        tweets = process_user(username)
        if tweets is not None:
            # Accumulate tweets and retweets in their respective lists
            tweets_batch.append(tweets)

        if (idx + 1) % batch_size == 0:
            process_batch(tweets_batch)
            tweets_batch = []
            save_processed_users(processed_users, path)

        processed_users.add(username)

    # Process the remaining data
    if tweets_batch != []:
        process_batch(tweets_batch)


if __name__ == "__main__":
    extract()
