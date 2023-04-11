import json
import sys
import pandas as pd
import ipdb
import yerbamate
import os
import tqdm

import numpy as np


def numpy_to_python(obj):
    if isinstance(obj, dict):
        return {k: numpy_to_python(v) for k, v in obj.items()}
    elif isinstance(obj, (np.ndarray, np.generic)):
        return obj.tolist()
    # Convert numpy number to native Python number
    elif isinstance(obj, np.number):
        return obj.item()
    # Convert lists recursively
    elif isinstance(obj, list):
        return [numpy_to_python(item) for item in obj]
    else:
        return obj


env = yerbamate.Environment()


# ipdb.set_trace()
save_path = os.path.join(env["save_path"], "users")

files = os.listdir(save_path)
total = 0
cnt = 0

# handle interrupt


def get_users():

    users = []
    tweets = 0
    rt = 0

    for dir in tqdm.tqdm(files):
        # ipdb.set_trace()
        # ipdb.set_trace()

        #   check if metadata exists

        tweets_path = os.path.join(
            env["save_path"], "users", dir, "tweets.parquet")
        retweets_path = os.path.join(
            env["save_path"], "users", dir, "retweets.parquet")

        if os.path.exists(tweets_path):

            # if empty, continue
            if os.stat(tweets_path).st_size == 0:
                continue

            df = pd.read_parquet(tweets_path)
            if df.shape[0] == 0:
                continue
            user = df['user'].iloc[0]
            # drop all user columns
            df = df.drop(columns=['user'])
            tweets += df.shape[0]

            users += [user]
            # users += [user]

            # size = df.shape[0]
            # total += size
            # ipdb.set_trace()
            if os.path.exists(retweets_path):
                # if empty, continue
                if os.stat(retweets_path).st_size == 0:
                    continue

                retweets_df = pd.read_parquet(retweets_path)
                # ipdb.set_trace()
                # df = pd.concat([df, retweets_df])
                rt += retweets_df.shape[0]

    print(f"Total tweets: {tweets}")
    print(f"Total retweets: {rt}")
    print(f"Total: {tweets + rt}")
    print("Total users: ", len(users))

    df = pd.DataFrame(users)

    # flat_users_df = pd.json_normalize(df['user'])
    # # ipdb.set_trace()

    # df['user_id'] = df['user'].apply(lambda x: x['id'])

    # df = df.drop_duplicates(subset=['user_id'])
    # flat_users_df = flat_users_df.drop_duplicates(subset=['id'])

    # try:
    #     merged_df = pd.merge(
    #         df, flat_users_df, left_on='user_id', right_on='id', how='left')
    # except:

    #     ipdb.set_trace()

    # # total tweets
    # total_tweets = merged_df['tweets_count'].sum().item()
    # total_users = merged_df.shape[0]
    # print(f"Total users: {total_users}")
    # print(f"Total tweets: {total_tweets}")
    # print(f"Total retweets: {total_retweets}")
    # return merged_df


if __name__ == "__main__":
    get_users()
