import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses


env = yerbamate.Environment()

# from .download import download_user


def parse_tweet_users(tweet):

    # ipdb.set_trace()

    uname = tweet.user["username"]

    # make a folder with this username

    save_path = os.path.join(env["save_path"], "users", uname)
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    tweet_path = os.path.join(env["save_path"], "users", uname, "tweets.parquet")

    if os.path.exists(tweet_path):
        
        if os.stat(tweet_path).st_size == 0:
            os.remove(tweet_path)
            print("Removed empty file:", tweet_path)
            
    else:
        pass


folder = "users"

for uname in os.listdir(os.path.join(env["save_path"], folder)):
    tweet_path = os.path.join(env["save_path"], "users", uname, "tweets.parquet")
    rt_path = os.path.join(env["save_path"], "users", uname, "retweets.parquet")
    # ipdb.set_trace()

    if os.path.exists(rt_path):

        if os.stat(rt_path).st_size == 0:
            os.remove(rt_path)
            print("Removed empty file:", rt_path)

    if os.path.exists(tweet_path):
        
        if os.stat(tweet_path).st_size == 0:
            os.remove(tweet_path)
            print("Removed empty file:", tweet_path)    
    else:
        pass