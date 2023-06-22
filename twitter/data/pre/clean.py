import sys
import warnings
import numpy as np
import tqdm
import yerbamate
import pandas as pd
import os
import shutil, ipdb
from multiprocessing import Pool
from .rtd_clean import preprocess_user as preprocess_user_rtd

env = yerbamate.Environment()


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


def clean_media(entry):
    if isinstance(entry, dict) or isinstance(entry, list) or entry is None:
        return entry
    else:
        return None


def clean_rows_to_int64(entry):
    # if nan, return NOne
    if pd.isna(entry) or pd.isnull(entry):
        return int(-1)

    return int(float(entry))


def process_tweet_df(tweets):
    int_rows = [
        "id",
        "replyCount",
        "retweetCount",
        "likeCount",
        "quoteCount",
        "conversationId",
        # "inReplyToTweetId",
    ]
    for col in int_rows:
        tweets[col] = tweets[col].apply(clean_rows_to_int64)
        # if dtype is not int64, convert to int64
        # tweets[col] = tweets[col].astype("Int64")

    tweets["inReplyToTweetId"] = tweets["inReplyToTweetId"].astype(pd.Int64Dtype())

    # clean media
    tweets["media"] = tweets["media"].apply(clean_media)

    tweets["links"] = tweets["links"].apply(clean_media)

    columns_to_keep = [
        "url",
        "id",
        "user",
        "date",
        "rawContent",
        "lang",
        "source",
        "sourceUrl",
        "sourceLabel",
        "links",
        "media",
        "retweetedTweet",
        "quotedTweet",
        "inReplyToTweetId",
        "inReplyToUser",
        "mentionedUsers",
        "hashtags",
        "conversationId",
        "replyCount",
        "retweetCount",
        "likeCount",
        "quoteCount",
    ]

    tweets = tweets[columns_to_keep]

    # we need to process quotedTweet and retweetedTweet if they are not null, this is an object, not a df

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        tweets.loc[:, "quotedTweet"] = tweets["quotedTweet"].apply(
            lambda x: process_tweet(x)
        )

    # ipdb.set_trace()

    # add quotedTweetId, quotedTweetUserId, quotedTweetUserScreenName
    tweets["quotedTweetId"] = (
        tweets["quotedTweet"]
        .apply(lambda x: x["id"] if x is not None else None)
        .astype(pd.Int64Dtype())
    )
    tweets["quotedTweetUserId"] = (
        tweets["quotedTweet"]
        .apply(
            lambda x: x["user"]["id"]
            if x is not None
            and isinstance(x, dict)
            and "user" in x
            and x["user"] is not None
            else None
        )
        .astype(pd.Int64Dtype())
    )
    # format column to be specifically object
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        tweets.loc[:, "retweetedTweet"] = tweets["retweetedTweet"].apply(
            lambda x: process_tweet(x)
        )

    # add retweetedTweetId, retweetedTweetUserId, retweetedTweetUserScreenName
    tweets["retweetedTweetId"] = (
        tweets["retweetedTweet"]
        .apply(lambda x: x["id"] if x is not None else None)
        .astype(pd.Int64Dtype())
    )
    # tweets["retweetedTweetUserId"] = tweets["retweetedTweet"].apply(
    #     lambda x: x["user"]["id"] if x is not None else None
    # )
    tweets["retweetedTweetUserId"] = (
        tweets["retweetedTweet"]
        .apply(
            lambda x: x["user"]["id"]
            if x is not None
            and isinstance(x, dict)
            and "user" in x
            and x["user"] is not None
            else None
        )
        .astype(pd.Int64Dtype())
    )

    tweets["user"] = tweets["user"].apply(lambda x: clean_user(x))

    # add userId
    tweets["userId"] = (
        tweets["user"]
        .apply(lambda x: x["id"] if x is not None else None)
        .astype(pd.Int64Dtype())
    )

    # fill na userId with first userId
    tweets["userId"] = tweets["userId"].fillna(method="ffill")

    # clean mentionedUsers
    tweets["mentionedUsers"] = tweets["mentionedUsers"].apply(
        lambda x: [clean_mentioned_users(user) for user in x] if x is not None else []
    )

    # add mentionedUserIds
    tweets["mentionedUserIds"] = tweets["mentionedUsers"].apply(
        lambda x: [int(user["id"]) for user in x] if x is not None else []
    )

    # inReplyToUserId
    # ipdb.set_trace()
    tweets["inReplyToUserId"] = (
        tweets["inReplyToUser"]
        .apply(lambda x: x["id"] if pd.notna(x) and x is not None else None)
        .astype(pd.Int64Dtype())
    )

    #

    return tweets


def clean_mentioned_users(entry):
    # only keep id, username, displayname
    if isinstance(entry, dict):
        return {
            "id": entry["id"],
            "username": entry["username"],
            "displayname": entry["displayname"],
        }
    else:
        return None


def process_tweet(tweet):
    if tweet is None or pd.isna(tweet) or pd.isnull(tweet):
        return None

    # ipdb.set_trace()
    # id to int64 if not already
    tweet["id"] = int(tweet["id"])

    # if key user does not exist, its a deleted/suspended tweet
    if "user" not in tweet.keys():
        columns_to_keep = [
            "url",
            "id",
            "user",
            "date",
            "rawContent",
            "lang",
            "source",
            "sourceUrl",
            "sourceLabel",
            "links",
            "media",
            "retweetedTweet",
            "quotedTweet",
            "inReplyToTweetId",
            "inReplyToUser",
            "mentionedUsers",
            "hashtags",
            "conversationId",
            "replyCount",
            "retweetCount",
            "likeCount",
            "quoteCount",
        ]
        # set to None for none existing items
        for col in columns_to_keep:
            if col not in tweet.keys():
                tweet[col] = None
        tweet = {k: v for k, v in tweet.items() if k in columns_to_keep}
        return tweet

    try:
        tweet["user"] = clean_user(tweet["user"])
    except:
        ipdb.set_trace()

    # tweet["retweetedTweet"] = convert_id_to_int64(tweet, "retweetedTweet")
    tweet["mentionedUsers"] = (
        [clean_mentioned_users(user) for user in tweet["mentionedUsers"]]
        if tweet["mentionedUsers"] is not None
        else []
    )
    tweet["inReplyToUser"] = clean_user(tweet["inReplyToUser"])
    # tweet["user"] = clean_user(tweet["user"])
    tweet["quotedTweet"] = process_tweet(tweet["quotedTweet"])
    tweet["retweetedTweet"] = process_tweet(tweet["retweetedTweet"])
    tweet["conversationId"] = (
        int(tweet["conversationId"]) if pd.notna(tweet["conversationId"]) else int(-1)
    )
    tweet["inReplyToTweetId"] = (
        int(tweet["inReplyToTweetId"])
        if pd.notna(tweet["inReplyToTweetId"])
        else int(-1)
    )

    tweet["media"] = clean_media(tweet["media"])
    tweet["links"] = clean_media(tweet["links"])

    columns_to_keep = [
        "url",
        "id",
        "user",
        "date",
        "rawContent",
        "lang",
        "source",
        "sourceUrl",
        "sourceLabel",
        "links",
        "media",
        "retweetedTweet",
        "quotedTweet",
        "inReplyToTweetId",
        "inReplyToUser",
        "mentionedUsers",
        "hashtags",
        "conversationId",
        "replyCount",
        "retweetCount",
        "likeCount",
        "quoteCount",
    ]

    tweet = {k: v for k, v in tweet.items() if k in columns_to_keep}

    int_rows = [
        "id",
        "replyCount",
        "retweetCount",
        "likeCount",
        "quoteCount",
        "conversationId",
        "inReplyToTweetId",
    ]

    for col in int_rows:
        tweet[col] = clean_rows_to_int64(tweet[col])

    # if tweet['conversationId'] == -1:
    #     ipdb.set_trace()
    # tweet["inReplyToTweetId"] = tweet["inReplyToTweetId"].astype(pd.Int64Dtype())

    return tweet


def clean_user(user):
    if user is None or pd.isna(user) or pd.isnull(user):
        return None
    # ipdb.set_trace()
    # # set profileBannerUrl and profileImageUrl to '' if nan
    # user["profileBannerUrl"] = (
    #     user["profileBannerUrl"] if pd.notna(user["profileBannerUrl"]) else ""
    # )
    # user["profileImageUrl"] = (
    #     user["profileImageUrl"] if pd.notna(user["profileImageUrl"]) else ""
    # )
    user["id"] = int(user["id"])

    try:
        if user["link"] is not None:
            # if str, pass
            if type(user["link"]) == str:
                pass
            else:
                user["link"] = (
                    user["link"]["url"]
                    if user["link"] != None
                    and pd.notnull(user["link"])
                    and pd.notna(user["link"])
                    else ""
                )
        else:
            user["link"] = ""
            # ipdb.set_trace()
            pass

            # user["link"] = pd.NA
    except:
        # if str, pass
        if type(user["link"]) == str:
            pass
        else:
            ipdb.set_trace()

        # if link str, pass
        # pass

    columns_to_keep = [
        "created",
        "displayname",
        "favouritesCount",
        "followersCount",
        "friendsCount",
        "id",
        "link",
        "listedCount",
        "location",
        "mediaCount",
        "protected",
        "rawDescription",
        "statusesCount",
        "username",
        "verified",
    ]

    user = {k: v for k, v in user.items() if k in columns_to_keep}

    return user


def preprocess_user(username):
    user_path = os.path.join(env["data"], "users", username)
    tweets_path = os.path.join(user_path, "tweets.parquet")
    retweets_path = os.path.join(user_path, "retweets.parquet")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # ipdb.set_trace()
        try:
            tweets = pd.read_parquet(tweets_path)
            tweets = process_tweet_df(tweets)
            # save tweets
            tweets.to_parquet(tweets_path)

            # if not tweets.dtypes.equals(schema):
            #     ipdb.set_trace()
        except:
            tweets = None
            if os.path.exists(tweets_path):
                print(f"Error reading {tweets_path}")
                print("Preprocessing user")
                preprocess_user_rtd(username)
                print("Done preprocessing user")

                if os.path.exists(tweets_path):
                    try:
                        tweets = pd.read_parquet(tweets_path)
                        tweets = process_tweet_df(tweets)
                        # save tweets
                        tweets.to_parquet(tweets_path)
                    except:
                        print(f"Error reading z {tweets_path}")
                        ipdb.set_trace()
                        pass

                return
                ipdb.set_trace()

        try:
            retweets = pd.read_parquet(retweets_path)
            retweets = process_tweet_df(retweets)
            # save retweets
            retweets.to_parquet(retweets_path)

            # if not retweets.dtypes.equals(schema):
            #     ipdb.set_trace()
        except:
            retweets = None
            if os.path.exists(retweets_path):
                print(f"Error reading {retweets_path}")
                print("Preprocessing user")

                preprocess_user_rtd(username)
                print("Done preprocessing user")

                if os.path.exists(retweets_path):
                    try:
                        retweets = pd.read_parquet(retweets_path)
                        retweets = process_tweet_df(retweets)
                        # save retweets
                        retweets.to_parquet(retweets_path)
                    except:
                        print(f"Error reading z {retweets_path}")
                        ipdb.set_trace()
                        pass

                return
                ipdb.set_trace()
            pass

    # ipdb.set_trace()

    if tweets is not None:
        return tweets.iloc[0]["user"]
    elif retweets is not None:
        return retweets.iloc[0]["user"]

    # ipdb.set_trace()


def process_not_multithreaded():
    users_path = os.path.join(env["data"], "users")
    users = [
        "fatisaaad",
        "simasnipper",
        "iran_____azad",
        "iweirrdoo",
        "harim10878735",
        "EmsSayar",
        "Existisda66",
        "mohmadslmni",
        "khedri_ako",
        "Farshad_8731",
        "Zahra61080597",
        "Abolfazl_sharr",
        "MNasehiyan",
        "itsnegry",
        "zeinab45031521",
        "Saeed_Aghebat",
        "Mohamma36788196",
        "JavadAh86733988",
        "Mariyan444",
        "ho3in_iran",
    ]

    ipdb.set_trace()

    # temprary fix

    tweets_df = pd.read_parquet(
        os.path.join(env["data"], "users", users[0], "tweets.parquet")
    )
    schmea = tweets_df.dtypes

    # Process users one by one
    user_objects_with_tweets_added = []
    for user in users:
        user_object = preprocess_user(user, schema=schmea)
        if user_object is not None:
            user_objects_with_tweets_added.append(user_object)

    # Filter out None values and create DataFrame
    user_objects_with_tweets_added = filter(None, user_objects_with_tweets_added)
    df_users = pd.DataFrame(user_objects_with_tweets_added)

    # You can save the DataFrame or perform further operations
    print(df_users)
    ipdb.set_trace()


def process_all_users(start=0):
    users_path = os.path.join(env["data"], "users")
    users = os.listdir(users_path)
    users = users[start:]
    # Use multiprocessing to preprocess users
    with Pool(os.cpu_count()) as pool:
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
    usernae = "armita_kh2"
    user_path = os.path.join(env["data"], "users", usernae)
    ipdb.set_trace()
    preprocess_user(usernae)


if __name__ == "__main__":
    # manual()
    # process_not_multithreaded()
    # process_all_users(408715)
    process_all_users()

# First 74040 users inReplyToTweetId is float64, the rest is int64
