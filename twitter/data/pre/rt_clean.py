import sys
import numpy as np
import tqdm
import yerbamate
import pandas as pd
import os
import shutil, ipdb
from multiprocessing import Pool

env = yerbamate.Environment()


def convert_id_to_int64(row, key):
    # Check if the nested column is not None and is a dictionary

    if row[key] and isinstance(row[key], dict):
        # Convert the 'id' field to int64
        user_id = row[key].get("id")
        if user_id is not None:
            row[key]["id"] = int(user_id)
        # Check if there's a nested 'user' object within the tweet

        conversation_id = row[key].get("conversationId")
        if conversation_id is not None:
            row[key]["conversationId"] = int(conversation_id)

        user_obj = row[key].get("user")
        if user_obj and isinstance(user_obj, dict):
            user_id = user_obj.get("id")
            if user_id is not None:
                user_obj["id"] = int(user_id)

        user_obj = row[key].get("inReplyToUser")
        if user_obj and isinstance(user_obj, dict):
            user_id = user_obj.get("id")
            if user_id is not None:
                user_obj["id"] = int(user_id)

        inreply_to_tweet_id = row[key].get("inReplyToTweetId")
        if inreply_to_tweet_id is not None:
            row[key]["inReplyToTweetId"] = int(inreply_to_tweet_id)

        inReply_toUser_id = row[key].get("inReplyToUserId")
        if inReply_toUser_id is not None:
            # if not nan
            if pd.notna(inReply_toUser_id):
                row[key]["inReplyToUserId"] = int(inReply_toUser_id)
        else:
            row[key]["inReplyToUserId"] = None

        # mentioned_users = row[key].get('mentionedUsers')
        # if mentioned_users and isinstance(mentioned_users, list):
        #  if has mentionedUsers, use conver_list_id_to_int64
        if row[key].get("mentionedUsers") is not None:
            row[key]["mentionedUsers"] = conver_list_id_to_int64(
                row[key], "mentionedUsers"
            )

        columns_TO_DROP = ["card", "cashtags", "vibe", "renderedContent", "viewCount"]

        for col in columns_TO_DROP:
            if row[key].get(col) is not None:
                row[key].pop(col, None)

        # if quotedTweet, use convert_id_to_int64
        if row[key].get("quotedTweet") is not None:
            row[key]["quotedTweet"] = convert_id_to_int64(row[key], "quotedTweet")

    # check if list

    return row[key]


def conver_list_id_to_int64(row, key):
    if (
        isinstance(row[key], (list, pd.core.series.Series, np.ndarray))
        and len(row[key]) > 0
    ):
        # id's to int

        for i in range(len(row[key])):
            user_id = row[key][i].get("id")
            if user_id is not None:
                row[key][i]["id"] = int(user_id)

    return row[key]

    return row[key]


# Convert ID inside inReplyToUser to int64


def apply_tweet_cleaning(tweets):
    # id to int64 if not already
    tweets["id"] = tweets["id"].astype("int64")

    tweets["retweetedTweet"] = tweets.apply(
        lambda row: convert_id_to_int64(row, "retweetedTweet"), axis=1
    )
    tweets["mentionedUsers"] = tweets.apply(
        lambda row: conver_list_id_to_int64(row, "mentionedUsers"), axis=1
    )
    tweets["inReplyToUser"] = tweets.apply(
        lambda row: convert_id_to_int64(row, "inReplyToUser"), axis=1
    )
    tweets["user"] = tweets.apply(lambda row: convert_id_to_int64(row, "user"), axis=1)
    tweets["quotedTweet"] = tweets.apply(
        lambda row: convert_id_to_int64(row, "quotedTweet"), axis=1
    )
    tweets["retweetedTweet"] = tweets.apply(
        lambda row: convert_id_to_int64(row, "retweetedTweet"), axis=1
    )
    tweets["conversationId"] = tweets["conversationId"].apply(
        lambda x: int(x) if pd.notnull(x) and pd.notna(x) else pd.NA
    )
    tweets["inReplyToTweetId"] = tweets["inReplyToTweetId"].apply(
        lambda x: int(x) if pd.notnull(x) and pd.notna(x) else pd.NA
    )

    # drop card, cashtags, vibe, renderedContent
    tweets = tweets.drop(
        ["card", "cashtags", "vibe", "renderedContent", "viewCount"],
        axis=1,
        errors="ignore",
    )
    return tweets


def preprocess_user(username):
    user_path = os.path.join(env["data"], "users", username)
    tweets_path = os.path.join(user_path, "tweets.parquet")
    retweets_path = os.path.join(user_path, "retweets.parquet")

    try:
        tweets = pd.read_parquet(tweets_path)
    except:
        tweets = pd.DataFrame()
    try:
        retweets = pd.read_parquet(retweets_path)
    except:
        retweets = pd.DataFrame()

    if tweets.empty and retweets.empty:
        shutil.rmtree(user_path)
        print(f"Removed user: {username}")
        return None

    if retweets.empty:
        # remove retweets.parquet if exists
        if os.path.exists(retweets_path):
            os.remove(retweets_path)

    else:
        na_retweets = retweets[retweets["retweetedTweet"].isna()]
        if not na_retweets.empty:
            # print("Fixing retweets.parquet")
            tweets = pd.concat([tweets, na_retweets], ignore_index=True)
            retweets = retweets.dropna(subset=["retweetedTweet"])

            # ipdb.set_trace()
            tweets = apply_tweet_cleaning(tweets)
            retweets = apply_tweet_cleaning(retweets)
            # mentionedUsers is nested object, so we use apply, we need convert_id_to_int64

            # we need to apply tweet_cleaning on quotedTweet since it's a nested object, but some of them are empty
            # so we need to check if it's empty or not
            # if not empty, apply tweet_cleaning
            # tweets['quotedTweet'] = tweets['quotedTweet'].apply(lambda x: apply_tweet_cleaning(x) if pd.notnull(x)  else x)

            try:
                tweets.to_parquet(tweets_path)
                retweets.to_parquet(retweets_path)

            except:
                print("user: ", username)
                ipdb.set_trace()

            try:
                user_object = tweets["user"].iloc[0]
                return user_object
            except:
                return retweets["user"].iloc[0]

        # check if tweets is empty
    if tweets.empty:
        # remove tweets.parquet if exists
        if os.path.exists(tweets_path):
            os.remove(tweets_path)

        # check if retweets has column card, if do, apply_tweet_cleaning
        if "card" in retweets.columns:
            retweets = apply_tweet_cleaning(retweets)

        retweets.to_parquet(retweets_path)

        return retweets["user"].iloc[0]
    else:
        # check if 'card' in tweets.columns, if do, apply_tweet_cleaning
        if "card" in tweets.columns:
            tweets = apply_tweet_cleaning(tweets)

            tweets.to_parquet(tweets_path)

        if "card" in retweets.columns:
            retweets = apply_tweet_cleaning(retweets)

            retweets.to_parquet(retweets_path)

        # check if it has quotedTweet, and if one of them is not empty, and has 'card' in it, apply_tweet_cleaning
        # check if there is one quotedTweet that is not None

        return tweets["user"].iloc[0]


def process_not_multithreaded():
    users_path = os.path.join(env["data"], "users")
    users = os.listdir(users_path)

    # Process users one by one
    user_objects_with_tweets_added = []
    for user in users:
        user_object = preprocess_user(user)
        if user_object is not None:
            user_objects_with_tweets_added.append(user_object)

    # Filter out None values and create DataFrame
    user_objects_with_tweets_added = filter(None, user_objects_with_tweets_added)
    df_users = pd.DataFrame(user_objects_with_tweets_added)

    # You can save the DataFrame or perform further operations
    print(df_users)


def process_all_users(start=0):
    users_path = os.path.join(env["data"], "users")
    users = os.listdir(users_path)
    users = users[start:]
    # Use multiprocessing to preprocess users
    with Pool(os.cpu_count() // 4) as pool:
        # Collect the user objects of users whose tweets were added
        user_objects_with_tweets_added = list(
            tqdm.tqdm(
                pool.imap(preprocess_user, users),
                total=len(users),
                desc="Processing users",
            )
        )

    # Filter out None values and create DataFrame
    user_objects_with_tweets_added = filter(None, user_objects_with_tweets_added)
    df_users = pd.DataFrame(user_objects_with_tweets_added)

    # You can save the DataFrame or perform further operations
    print(df_users)


def manual():
    usernae = "AQ_jbril77"
    user_path = os.path.join(env["data"], "users", usernae)
    ipdb.set_trace()
    preprocess_user(usernae)


if __name__ == "__main__":
    # manual()
    # process_not_multithreaded()
    process_all_users(408715)
    # process_all_users()

# First 74040 users inReplyToTweetId is float64, the rest is int64
