import json
import sys
import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses, tqdm

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

    for dir in tqdm.tqdm(files):
        # ipdb.set_trace()
        # ipdb.set_trace()

        #   check if metadata exists
        if os.path.exists(os.path.join(env["save_path"], "users",dir, "metadata.json")):
            try:
                metadata = json.load(open(os.path.join(env["save_path"], "users", dir, "metadata.json")))
            except:
                # remove file
                os.remove(os.path.join(env["save_path"], "users",dir, "metadata.json"))
                metadata = None

        tweets_path = os.path.join(env["save_path"], "users", dir, "tweets.parquet")
        retweets_path = os.path.join(env["save_path"], "users", dir, "retweets.parquet")

        if os.path.exists(tweets_path):

            last_modified = os.path.getmtime(tweets_path)

            if metadata != None and metadata["date"] >= last_modified:
                # print("Skipping", dir, "because it is already up to date.")

                # check if retweets are up to date
                if os.path.exists(retweets_path):
                    last_modified = os.path.getmtime(retweets_path)
                    if metadata["date"] >= last_modified:
                        # print("Skipping", dir, "because it is already up to date.")
                        users.append(metadata)
                        continue

            # if empty, continue
            if os.stat(tweets_path).st_size == 0:
                continue

            try:
                df = pd.read_parquet(tweets_path)
                user = df["user"][0]

                # if type series, duplicates are there, so remove them
                if type(user) == pd.core.series.Series:
                    user = user.drop_duplicates().iloc[0]
            except:
                # ipdb.set_trace()
                # probably empty
                continue
            # get total likes
            likes = df["likeCount"].sum()
            # get total retweets
            # change to python number
            # ipdb.set_trace()
            # likes = likes

            tweet_count = df.shape[0]

            if os.path.exists(retweets_path):

                if os.stat(retweets_path).st_size == 0:

                    retweets = 0
                    # continue
                else:
                    df = pd.read_parquet(retweets_path)
                    retweets = df.shape[0]

            else:
                retweets = 0

            metadata = {
                "tweets_count": tweet_count,
                "retweets_count": retweets,
                "likes_count": likes.item(),
                "date": last_modified,
                "user": user,
            }

            metadata["user"]["created"] = metadata["user"]["created"].strftime(
                    "%Y-%m-%dT%H:%M:%S%z"
                )
            
            # ipdb.set_trace()

            metadata = numpy_to_python(metadata)

            # ipdb.set_trace()
            # if link in metadata["user"]["link"]:, link['indices'] = are np.array, convert to list

            try:

                json.dump(metadata, open(os.path.join(save_path, dir, "metadata.json"), "w"))
            except:
                continue

            users += [metadata]

    df = pd.DataFrame(users)


    flat_users_df = pd.json_normalize(df['user'])

    df['user_id'] = df['user'].apply(lambda x: x['id'])

    df = df.drop_duplicates(subset=['user_id'])
    flat_users_df = flat_users_df.drop_duplicates(subset=['id'])

    try:
        merged_df = pd.merge(df, flat_users_df, left_on='user_id', right_on='id', how='left')
    except:

        ipdb.set_trace()

    # total tweets
    total_tweets = merged_df['tweets_count'].sum().item()
    total_users = merged_df.shape[0]
    print(f"Total users: {total_users}")
    print(f"Total tweets: {total_tweets}")

    return merged_df

