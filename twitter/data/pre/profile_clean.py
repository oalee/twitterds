import concurrent
import shutil
from .id_clean_prepro import id_prepro
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
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

import filelock
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


def save_profile_to_pickle(user, path):
    with open(path, 'wb') as f:
        pickle.dump(user, f)


def append_to_parquet_file(input_file, new_data):

    lock_file = input_file + ".lock"

    with filelock.FileLock(lock_file, timeout=30):
        if os.path.exists(input_file):
            try:
                old_data = pd.read_parquet(input_file)
                new_data = pd.concat([old_data, new_data], ignore_index=True)
                # drop duplicates
                new_data = new_data.drop_duplicates(
                    subset=['id'], keep='first')
            except Exception as e:
                print(f"Error reading parquet file {input_file}: {e}")
                return

        new_data.to_parquet(input_file, index=False)


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


def get_or_create_user_directory(user_name, is_new_user=False):

    base_dir = env["data"]
    user_type = "new_users" if is_new_user else "users"
    user_dir = os.path.join(base_dir, user_type, user_name)
    os.makedirs(user_dir, exist_ok=True)
    return user_dir


def drop_obj_column(df):
    return df.drop(
        columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers', 'user'], errors='ignore')


# saves self user to pickle
def save_self_user_profiles(user):
    pass

# saves new user df, it includes tweets that needs to be saved as new_users/{user}/tweets.parquet
# read the df if existing and updates (no duplicates)
# then saves the user profile pickle if not exist


def save_new_user_profile(user, tweets_df, base_path):
    user_path = os.path.join(base_path, user['username'])

    # Create user directory if it doesn't exist
    if not os.path.exists(user_path):
        os.makedirs(user_path)

    # Save user profile
    profile_path = os.path.join(user_path, 'profile.pickle')
    if not os.path.exists(profile_path):
        save_profile_to_pickle(user, profile_path)

    # Save user tweets
    tweets_path = os.path.join(user_path, 'tweets.parquet')

    append_to_parquet_file(tweets_path, tweets_df)
    # lock_file = tweets_path + ".lock"

    # if os.path.exists(tweets_path):
    #     existing_tweets_df = pd.read_parquet(tweets_path)
    #     updated_tweets_df = pd.concat(
    #         [existing_tweets_df, tweets_df]).drop_duplicates(subset=['id'])
    # else:
    #     updated_tweets_df = tweets_df
    # updated_tweets_df.to_parquet(tweets_path)


def process_profile(username, user_df):

    # get rewtweetedTweetUserNames

    rts = user_df[pd.notnull(user_df['retweetedTweet'])]['retweetedTweet']
    rt_user_names = rts.apply(
        lambda x: x['user']['username'] if x is not None else None)

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

    try:
        self_user = unique_users.to_list()[0]
    except:
        # no self_user found
        self_user = None
        # ipdb.set_trace()

    path = os.path.join(env['sv_path'], 'users',
                        username, 'profile.pickle')

    if self_user is not None:
        save_profile_to_pickle(self_user, path)

    # ipdb.set_trace()

    flat_mt_users = [user for sublist in mt_user_names for user in (
        sublist if sublist is not None else [])]

    all_user_names = pd.concat(
        [rt_user_names, qt_user_names, ir_user_names, pd.Series(flat_mt_users)]
    ).drop_duplicates().to_list()

    global userList

    # users_not_in_gu = [user for user in all_user_names if user not in userList]
    # users_in_gu = [user for user in all_user_names if user in userList]

    new_tweets = pd.concat([rts, not_none_quoted_tweets])
    new_tweets = new_tweets.apply(pd.Series)

    # ipdb.set_trace()

    # if new_tweets is not empty, process
    try:
        if not new_tweets.empty:
            new_tweets = new_tweets.dropna(subset=['user'])
            new_tweets['username'] = new_tweets['user'].apply(
                lambda x: x['username'])

            new_tweets_not_gu = new_tweets[~new_tweets['username'].isin(userList)]
            grouped_tweets_not_gu = new_tweets_not_gu.groupby('username')

            for _, tweets_df in grouped_tweets_not_gu:
                user_profile = tweets_df.iloc[0]['user']
                df = drop_obj_column(id_prepro(tweets_df))
                # drop username column
                df.drop(columns=['username'], inplace=True)
                save_new_user_profile(
                    user_profile, df, os.path.join(env['data'], 'new_users'))

    except Exception as e:
        print(e)
        # ipdb.set_trace()
    # drop new_tweets users that are in gu

    # now, for the users not in gu, I need to
    # for rts in user_df, set rawContent to None

    user_df.loc[user_df['retweetedTweet'].notnull(), 'rawContent'] = None

    cleaned = drop_obj_column(user_df)

    uname = username  # $self_user['username']
    assert uname != None, "Username must be found"

    # rewrite self user tweets, remove retweets.parquet (now merged)

    tw_path = os.path.join(env["data"], "users", uname, "tweets.parquet")
    rt_path = os.path.join(env["data"], "users", uname, "retweets.parquet")

    cleaned.to_parquet(tw_path)

    if os.path.exists(rt_path):
        os.remove(rt_path)

    return self_user
    # save cleanded to tweets.parquet

    # if os.path.exists(tw_path):
    #     os.remove(tw_path)

    # now, the self_user profile needs to get linked to rtw,
    # get their rts, drop the rawContent column, url, and


def process_user(username):
    # username, idx = args
    user_df = get_user(username)

    if user_df is None or user_df.empty:
        return None  # , None

    # if no user column, then already processed
    if 'user' not in user_df.columns:
        print("Already processed user: ", username)
        # ipdb.set_trace()
        return None  # , None

    # ipdb.set_trace()
    user_df = id_prepro(user_df)  # preprocess tweets and add ids

    # if no user column, then, something went wrong
    # maybe the user is not available anymore
    # in any case, we can save the tweets and retweets
    # and move on to the next user

    profiles = process_profile(username, user_df)

    # user_df = user_df.drop(
    #     columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers', 'user'])

    # # ipdb.set_trace()
    # retweets = user_df[user_df['retweetedTweetId'].notnull()]
    # tweets = user_df[user_df['retweetedTweetId'].isnull()]

    return profiles  # tweets, retweets


def check_cleaned_user(username):
    path = os.path.join(env['data'], 'users', username, 'tweets.parquet')

    # if empty tweets, then delete file
    if os.path.exists(path) and os.path.getsize(path) == 0:
        os.remove(path)

    if not os.path.exists(path):
        path = os.path.join(env['data'], 'users', username, 'retweets.parquet')

        # if empty, then delete folder
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            shutil.rmtree(os.path.join(env['data'], 'users', username))
            print("Deleted empty user: ", username)
            return True

    df = vaex.open(path)

    # if card in columns, then it's not cleaned
    if 'card' in df.columns:
        return False
    else:
        return True


def get_unprocessed_users():

    # checks if the user has been processed with ckeck_cleaned_user
    # if not, then it's a new user

    users_list = get_userlist()

    # make this tqdm
    unprocessed_users = []
    for user in tqdm.tqdm(users_list):
        if not check_cleaned_user(user):
            unprocessed_users.append(user)
    # unprocessed_users = [
    #     user for user in users_list if not check_cleaned_user(user)]

    return unprocessed_users


def clean_again():

    unproccessed_users = get_unprocessed_users()

    # ipdb.set_trace()

    for user in tqdm.tqdm(unproccessed_users):
        print("Processing user: ", user)
        process_user(user)


def extract():
    users_list = get_userlist()

    path = os.path.join(env['sv_path'], 'profile_p_users.pickle')

    processed_users = load_processed_users(path)
    print("Already ", len(processed_users), "processed users")
    batch_size = 64  # Adjust this value based on your system's memory constraints
    process_size = 8  # Adjust this value based on your system's memory constraints
    unprocessed_users = [
        username for username in users_list if username not in processed_users]

    tweets_batch = []
    retweets_batch = []

    # handle the case where the process was interrupted

    with ProcessPoolExecutor(max_workers=process_size) as executor:
        futures = {executor.submit(process_user, (username, idx))
                                   : idx for idx, username in enumerate(unprocessed_users)}
        for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(unprocessed_users)):
            idx = futures[future]
            try:
                result_idx, profiles = future.result()
                assert idx == result_idx

                # if profiles is not None:
                # if profiles is not None:
                #     tweets_batch.append(profiles)
                # print("Processed user: ", unprocessed_users[idx])

                if (idx + 1) % batch_size == 0:
                    # process_batch(tweets_batch)
                    # tweets_batch = []
                    save_processed_users(processed_users, path)

                processed_users.add(unprocessed_users[idx])

            except Exception as e:
                print(f"Error processing user at index {idx}: {e}")
                ipdb.set_trace()

    # for idx, username in tqdm.tqdm(enumerate(unprocessed_users), total=len(unprocessed_users)):

    #     tweets = process_user(username)
    #     if tweets is not None:
    #         # Accumulate tweets and retweets in their respective lists
    #         tweets_batch.append(tweets)

    #     if (idx + 1) % batch_size == 0:
    #         # process_batch(tweets_batch)
    #         # tweets_batch = []
    #         save_processed_users(processed_users, path)

    #     processed_users.add(username)

    # # Process the remaining data
    # if tweets_batch != []:
    #     process_batch(tweets_batch)


if __name__ == "__main__":
    clean_again()
    # extract()
