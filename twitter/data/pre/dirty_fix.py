import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd


env = yerbamate.Environment()


users_path = os.path.join(env["data"], "users")
wd_path = "/run/media/al/wd/td/users/"

users = os.listdir(users_path)[:20]

manual_users = [
    'harim10878735',
    'EmsSayar',
    'mohmadslmni',
    'fatisaaad',
    'Zahra61080597',
    'simasnipper',
    'MNasehiyan',
    'itsnegry',
    'JavadAh86733988',
    'iran_____azad',
]

users = manual_users
# go throw users tweets and retweets parquet, if df['user'] is None, then replace it from wd_path

for user in users:
    tweets_path = os.path.join(users_path, user, "tweets.parquet")
    retweets_path = os.path.join(users_path, user, "retweets.parquet")

    try:
        wd_tweets = pd.read_parquet(os.path.join(wd_path, user, "tweets.parquet"))

        replacement_value = wd_tweets.iloc[0]["user"]
    except:
        replacement_value = None
        wd_retweets = pd.read_parquet(os.path.join(wd_path, user, "retweets.parquet"))
        replacement_value = wd_retweets.iloc[0]["user"]

    try:
        tweets = pd.read_parquet(tweets_path)

        if tweets["user"].isnull().sum() > 0:
            # fill NaN
            tweets["user"] = tweets["user"].apply(lambda x: replacement_value if pd.isna(x) else x)
            # tweets["user"] = tweets["user"].fillna(replacement_value)
            # ipdb.set_trace()
            print("replaced tweets")
            # save
            tweets.to_parquet(tweets_path)
    except:
        pass

    try:
        retweets = pd.read_parquet(retweets_path)

        if retweets["user"].isnull().sum() > 0:

            # retweets["user"] = retweets["user"].fillna(replacement_value)
            tweets["user"] = tweets["user"].apply(lambda x: replacement_value if pd.isna(x) else x)
            print("replaced retweets")
            # save
            retweets.to_parquet(retweets_path)

    except:
        pass
