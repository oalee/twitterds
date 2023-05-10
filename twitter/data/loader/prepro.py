from IPython.display import Markdown, display
import hashlib
from pathlib import Path
import json
import random
import sys
import pandas as pd
import ipdb
import yerbamate
import os
import tqdm
import re

import numpy as np

import subprocess


def get_userlist():
    path = os.path.join(env["data"], "users")
    dirs = os.listdir(path)
    # check if tweets exist

    # s.path.exists(os.path.join(path, "tweets.parquet")) or retwets:

    return dirs


def get_random_user():
    users = get_userlist()
    user = random.choice(users)
    return get_user(user)


def get_cleaned_user(user_name):

    path = os.path.join(env["data"], "users", user_name)

    df = None

    # check if tweets exist
    if os.path.exists(os.path.join(path, "tweets.parquet")):
        # check if not empty
        try:
            df = pd.read_parquet(os.path.join(path, "tweets.parquet"))
        except:
            df = None
    return df


def get_user(user_name):

    path = os.path.join(env["data"], "users", user_name)
    df = None
    rdf = None
    # check if tweets exist
    if os.path.exists(os.path.join(path, "tweets.parquet")):
        # check if not empty

        try:
            df = pd.read_parquet(os.path.join(path, "tweets.parquet"))
        except:
            df = None

    if os.path.exists(os.path.join(path, "retweets.parquet")):
        # check if not empty
        try:
            rdf = pd.read_parquet(os.path.join(path, "retweets.parquet"))
        except:
            rdf = None

    if df is not None and rdf is not None:
        df = pd.concat([df, rdf], ignore_index=True)
    elif df is None and rdf is not None:
        df = rdf

    return df


def clean_text(text):
    # Remove mentions
    text = re.sub(r'@\w+', '', text)

    # Remove URLs
    text = re.sub(r'http\S+|https\S+', '', text)

    # Remove newlines
    text = text.replace('\n', ' ')

    # Remove extra whitespaces
    text = re.sub(r'\s+', ' ', text).strip()

    # Remove "RT :"
    text = re.sub(r'RT :', '', text)

    return text


def get_cleaned_tweets(user_name):
    user = get_user(user_name)
    # Apply the cleaning function to the DataFrame
    user['cleanedContent'] = user['rawContent'].apply(clean_text)
    # return cleaned tweets
    return user['cleanedContent']


def get_user_df(user_name):
    user = get_user(user_name)
    if user is None:
        return None
    # Apply the cleaning function to the DataFrame
    user['cleanedContent'] = user['rawContent'].apply(clean_text)
    # return cleaned tweets
    return user


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
save_path = os.path.join(env["data"], "users")

files = os.listdir(save_path)
total = 0
cnt = 0
errs = 0
# handle interrupt


data_path = env['data']


def get_directory_size_with_du(path=data_path):
    cmd = ["du", "-sh", path]
    output = subprocess.check_output(cmd)
    size = output.strip().split()[0].decode('utf-8')
    return size


user_dir = Path(env["data"]) / "users"


def get_size(path):
    return os.path.getsize(path)


def hash_username(username, algorithm="sha1"):
    hash_obj = hashlib.new(algorithm)
    hash_obj.update(username.encode("utf-8"))
    return hash_obj.hexdigest()[:6]


def visualize_random_users(directory, num_users=10):
    all_users = [user for user in os.listdir(
        directory) if os.path.isdir(os.path.join(directory, user))]
    random_users = random.sample(all_users, min(num_users, len(all_users)))

    for user in random_users:
        hashed_user = hash_username(user)
        print(f"User: {hashed_user}")

        tweets_file = directory / user / "tweets.parquet"
        retweets_file = directory / user / "retweets.parquet"

        if tweets_file.is_file():
            print(f"  {tweets_file.name}: {get_size(tweets_file)} bytes")
        else:
            print(f"  {tweets_file.name}: File not found")

        if retweets_file.is_file():
            print(f"  {retweets_file.name}: {get_size(retweets_file)} bytes")
        else:
            print(f"  {retweets_file.name}: File not found")

        print()


def display_data_stats(users):
    # Define variables
    total_users = len(users)
    total_tweets = users.tweets_count.sum()

    # Create a markdown table
    markdown_table = f"""
    |       Metric       |       Value        |
    |--------------------|--------------------|
    | Total Users        |    {total_users}   |
    | Total Tweets       |    {total_tweets}  |
    | Size (Raw Data)    |     {total_size}   |
    """

    # Display the markdown table
    display(Markdown(markdown_table))

# directory_size = get_directory_size_with_du(data_path)
# print(f"Size of the data directory: {directory_size} bytes")


def get_train_sentences(size=1000000):

    # get all users, and their tweets
    tweets = []
    for dir in tqdm.tqdm(files):
        df = get_user_df(dir)
        try:
            tweets += df['cleanedContent'].tolist()

            # ipdb.set_trace()
            if len(tweets) >= size:
                break
        except:
            continue
        # print(len(tweets))

    return tweets


def get_users():

    users = []
    errs = 0
    empty = 0

    udf = pd.DataFrame()

    for dir in tqdm.tqdm(files):
        # ipdb.set_trace()
        # ipdb.set_trace()

        #   check if metadata exists
        if os.path.exists(os.path.join(env["data"], "users", dir, "metadata.json")):
            try:
                metadata = json.load(
                    open(os.path.join(env["data"], "users", dir, "metadata.json")))
            except:
                # remove file
                os.remove(os.path.join(
                    env["data"], "users", dir, "metadata.json"))
                metadata = None

        else:
            metadata = None

        tweets_path = os.path.join(
            env["data"], "users", dir, "tweets.parquet")
        retweets_path = os.path.join(
            env["data"], "users", dir, "retweets.parquet")

        # if dir == "aydin091":
        #     ipdb.set_trace()

        if os.path.exists(tweets_path):

            last_modified = os.path.getmtime(tweets_path)

            if metadata != None and metadata["date"] >= last_modified:
                # print("Skipping", dir, "because it is already up to date.")

                # check if retweets are up to date
                if os.path.exists(retweets_path):
                    last_modified = os.path.getmtime(retweets_path)
                    if metadata["date"] >= last_modified:
                        # print("Skipping", dir, "because it is already up to date.")
                        # check if metadata has all the fields
                        if "likes_count" in metadata and "tweets_count" in metadata:
                            users += [metadata]
                            # print("Skipping", dir, "because it is already up to date.")
                            continue

                        # users += [metadata]
                        # # print("Skipping", dir, "because it is already up to date.")
                        # continue

            # if empty, continue
            if os.stat(tweets_path).st_size == 0:
                empty += 1
                continue

            try:
                df = pd.read_parquet(tweets_path)

                if (df.shape[0] == 0):
                    empty += 1
                    continue

                user = df["user"][0]
                likes = df["likeCount"].sum().item()
                # if type series, duplicates are there, so remove them
                if type(user) == pd.core.series.Series:
                    user = user.drop_duplicates().iloc[0]
            except:
                # ipdb.set_trace()
                # probably empty
                # error
                errs += 1
                continue
            # get total likes
            tweet_count = df.shape[0]

            if os.path.exists(retweets_path):

                if os.stat(retweets_path).st_size == 0:

                    retweets = 0
                    # continue
                else:
                    try:
                        df = pd.read_parquet(retweets_path)
                        retweets = df.shape[0]
                    except:
                        retweets = 0

            else:
                retweets = 0
            # try:

            # except:
            #     ipdb.set_trace()

            if tweet_count == 0 and retweets == 0:
                continue
            metadata = {
                "tweets_count": tweet_count,
                "retweets_count": retweets,
                "likes_count": likes,
                # last modified date of tweets and check if retweets are up to date
                "date": last_modified,
                "created_date": user["created"].strftime(
                    "%Y-%m-%dT%H:%M:%S%z"
                ),
                "followers_count": user["followersCount"],
                "friends_count": user["friendsCount"]
            }

            # metadata["user"]["created"] = metadata["user"]["created"].strftime(
            #     "%Y-%m-%dT%H:%M:%S%z"
            # )

            # ipdb.set_trace()

            # metadata = numpy_to_python(metadata)

            # ipdb.set_trace()
            # if link in metadata["user"]["link"]:, link['indices'] = are np.array, convert to list

            try:

                json.dump(metadata, open(os.path.join(
                    env["data"], "users", dir, "metadata.json"), "w"))
            except:
                continue

            users += [metadata]

            # every 1000 users, save to udf
            if len(users) % 1000 == 0:
                udf = pd.concat([udf, pd.DataFrame(users)])
                users = []

    df = udf  # ;pd.DataFrame(users)

    # ipdb.set_trace()

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

    # total tweets
    # total_tweets = merged_df['tweets_count'].sum().item()
    # total_users = merged_df.shape[0]

    print(f"Total users: {df.shape[0]}")
    # print("Total empty files: ", empty)
    # print("Total errors: ", errs)

    print(f"Total tweets: {df['tweets_count'].sum().item()}")
    print(f"Total retweets: {df['retweets_count'].sum().item()}")

    return df


# if __nam__ == "__main__":
#     get_users()
#     ipdb.set_trace()
# e
