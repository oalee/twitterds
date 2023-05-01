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

from dask.diagnostics import ProgressBar
from dask.distributed import Client
import os
from dask import delayed

env = Environment()


# Replace filepath with the path to the pickle file
p_path = os.path.join(env["data"], "time", "processed_users.pkl")


# dask df indexed by month_tweets.parquet and month_retweets.parquet
#


def process_and_save_batch(users_batch, output_path, metadata):
    # Read and preprocess data in parallel using Dask delayed
    delayed_data = [dd.from_delayed(delayed(process_user)(
        username), meta=metadata) for username in users_batch]

    # Concatenate the data into a single Dask DataFrame
    combined_data = dd.concat(
        [df for df in delayed_data if df is not None], axis=0)

    # Optimize the Dask task graph for better performance
    combined_data = combined_data.persist()

    # Save the data to Parquet format, partitioned by month_year and user
    with ProgressBar():
        combined_data.to_parquet(output_path, partition_on=['month_year'])


def save_processed_users(processed_users, filepath=p_path):
    with open(filepath, 'wb') as f:
        pickle.dump(processed_users, f)


def load_processed_users(filepath=p_path):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'rb') as f:
        return pickle.load(f)


def clean_content(content):
    if not isinstance(content, str):
        content = str(content)
    return clean_text(content)


def safe_str_int(value):
    try:
        return str(np.int64(value))
    except (TypeError, ValueError):
        return None


def safe_int(value):
    try:
        return np.int64(value)
    except (TypeError, ValueError):
        return None


def id_prepro(user_df):
    if user_df.empty:
        return None

    user_df['month_year'] = user_df['date'].dt.strftime('%Y-%m')
    user_df['text'] = user_df['rawContent'].apply(clean_content)

    user_df = user_df[user_df['user'].notnull()].copy()

    remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates',
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

    user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
        lambda x: [str(user['id']) for user in x] if x is pd.notna(x) else None)

    # convert all ids to strings
    # for col in ['userId', 'retweetedTweetId', 'retweetedUserId', 'quotedTweetId', 'inReplyToUserId', 'inReplyToUserId', 'inReplyToTweetId']:
    #     user_df[col] = user_df[col].apply(
    #         lambda x: str(x) if pd.notna(x) else None)

    # viewCount safe int
    user_df['viewCount'] = pd.to_numeric(
        user_df['viewCount'], downcast='integer')
    # do this on Ids

    id_cols = ['userId', 'retweetedTweetId', 'retweetedUserId',
               'quotedTweetId', 'inReplyToUserId', 'inReplyToTweetId']

    for col in id_cols:
        user_df[col] = pd.to_numeric(
            user_df[col], errors='coerce', downcast='integer')

    user_df['hashtags'] = user_df['hashtags'].apply(lambda x: json.dumps(
        x.tolist()) if isinstance(x, np.ndarray) else json.dumps(x))    # ipdb.set_trace()

    return user_df


def process_user(username):
    user_df = get_user(username)

    if user_df is None or user_df.empty:
        empty_data = {
            'url': pd.Series([], dtype='object'),
            'date': pd.Series([], dtype='datetime64[ns, UTC]'),
            'rawContent': pd.Series([], dtype='object'),
            'id': pd.Series([], dtype='Int64'),
            'replyCount': pd.Series([], dtype='Int64'),
            'retweetCount': pd.Series([], dtype='Int64'),
            'likeCount': pd.Series([], dtype='Int64'),
            'quoteCount': pd.Series([], dtype='Int64'),
            'conversationId': pd.Series([], dtype='Int64'),
            'lang': pd.Series([], dtype='object'),
            'media': pd.Series([], dtype='bool'),
            'inReplyToTweetId': pd.Series([], dtype='Int64'),
            'hashtags': pd.Series([], dtype='object'),
            'viewCount': pd.Series([], dtype='Int64'),
            'month_year': pd.Series([], dtype='object'),
            'text': pd.Series([], dtype='object'),
            'userId': pd.Series([], dtype='Int64'),
            'retweetedTweetId': pd.Series([], dtype='Int64'),
            'retweetedUserId': pd.Series([], dtype='Int64'),
            'quotedTweetId': pd.Series([], dtype='Int64'),
            'inReplyToUserId': pd.Series([], dtype='Int64'),
            'mentionedUserIds': pd.Series([], dtype='object'),
        }

        return pd.DataFrame(empty_data)

    user_df = id_prepro(user_df)

    # drop object columns
    user_df = user_df.drop(columns=['user', 'retweetedTweet', 'quotedTweet',
                           'inReplyToUser', 'mentionedUsers'], errors='ignore')

    # Reorder columns to match the metadata
    columns_order = [
        'url', 'date', 'rawContent', 'id', 'replyCount', 'retweetCount',
        'likeCount', 'quoteCount', 'conversationId', 'lang', 'media',
        'inReplyToTweetId', 'hashtags', 'viewCount', 'month_year', 'text',
        'userId', 'retweetedTweetId', 'retweetedUserId', 'quotedTweetId',
        'inReplyToUserId', 'mentionedUserIds'
    ]
    user_df = user_df[columns_order]

    return user_df


def process_large_data(batch_size=1000):

    users_list = get_userlist()
    processed_users = load_processed_users()

    print("Already", len(processed_users), "processed users")

    unprocessed_users = [
        username for username in users_list if username not in processed_users]

    # Set up Dask distributed client (optional, but helpful for parallel processing)
    client = Client()

    # meta = process_user(users_list[10])

    metadata = {
        'url': 'str',
        'date': 'datetime64[ns, UTC]',
        'rawContent': 'str',
        'id': 'int64',
        'replyCount': 'int64',
        'retweetCount': 'int64',
        'likeCount': 'int64',
        'quoteCount': 'int64',
        'conversationId': 'int64',
        'lang': 'str',
        'media': 'bool',
        'inReplyToTweetId': 'Int64',  # Update the data type
        'hashtags': 'str',
        'viewCount': 'Int64',  # Update the data type
        'month_year': 'str',
        'text': 'str',
        'userId': 'Int64',  # Update the data type
        'retweetedTweetId': 'Int64',  # Update the data type
        'retweetedUserId': 'Int64',  # Update the data type
        'quotedTweetId': 'Int64',  # Update the data type
        'inReplyToUserId': 'Int64',  # Update the data type
        'mentionedUserIds': 'str',
    }

    print("Processing", len(unprocessed_users), "users")
    # Read and preprocess data in parallel using Dask delayed

    output_path = os.path.join(env['sv_path'], 'time')

    # Process and save data in batches
    for i in tqdm.trange(len(processed_users), len(users_list), batch_size):
        users_batch = unprocessed_users[i:i + batch_size]
        process_and_save_batch(users_batch, output_path, metadata)
        processed_users = processed_users.union(users_batch)

        save_processed_users(processed_users)

    remaining_users = len(unprocessed_users) - len(processed_users)
    remain_batch = unprocessed_users[-remaining_users:]
    process_and_save_batch(remain_batch, output_path, metadata)
    processed_users = processed_users.union(remain_batch)

    # delayed_data = [dd.from_delayed(delayed(process_user)(
    #     username), meta=metadata) for username in unprocessed_users]

    # print("Processing delayed ", len(delayed_data), "users")

    # Concatenate the data into a single Dask DataFrame
    # Clean up the Dask client resources
    client.close()


if __name__ == "__main__":
    process_large_data()
