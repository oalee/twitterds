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

    if not os.path.exists(tweet_path):
        # touch the file
        open(tweet_path, "a").close()

        df = pd.DataFrame()

        scraper = twitter.TwitterUserScraper(uname)
        # ipdb.set_trace()
        in_memory = []
        for i, tweet in enumerate(scraper.get_items()):
            try:
                print(i, tweet.url)
            except:
                pass
            di = dataclasses.asdict(tweet)
            in_memory.append(di)

            # save every 1000 tweets
            if len(in_memory) > 1000:
                df = df.append(in_memory, ignore_index=True)
                # df.to_parquet(tweet_path)
                in_memory = []
                # df = pd.DataFrame()

        df = df.append(in_memory, ignore_index=True)

        df.to_parquet(tweet_path)
        # df.to_csv(os.path.join(env["save_path"], "users", uname, "tweets.csv"))

    else:

        df = pd.read_parquet(tweet_path)

        # for i, tweet in enumerate(scraper.get_items()):
        #     try:
        #         print(i, tweet.url)
        #     except:
        #         continue
        #     di = dataclasses.asdict(tweet)

        #     # chekc if di["url"] is in df["url"
        #     # if not, then add it to the df
        #     # else if this is existing then stop iterating

        # now, rw.parquet the file

        # now check if this tweet is not in the retweets.parquet
        # if not, then add it to the retweets.parquet
        # else if this is existing then stop iterating

    # now, rw.parquet the file

    # df = pd.read_csv(os.path.join(env["save_path"], "users", uname, "tweets.csv"))

    # now, rw.parquet the file
    rt_parquet_path = os.path.join(env["save_path"], "users", uname, "retweets.parquet")
    if not os.path.exists(rt_parquet_path):
        scraper = twitter.TwitterProfileScraper(uname)

        open(rt_parquet_path, "a").close()

        rdf = pd.DataFrame()

        in_memory = []

        try:
            for i, tweet in enumerate(scraper.get_items()):
                try:
                    print(i, tweet.url)
                except:
                    pass
                di = dataclasses.asdict(tweet)
                in_memory.append(di)

                # save every 1000 tweets
                if len(in_memory) > 1000:
                    rdf = rdf.append(in_memory, ignore_index=True)
                    # df.to_parquet(tweet_path)
                    in_memory = []
                    # df = pd.DataFrame()

            rdf = rdf.append(in_memory, ignore_index=True)

        except Exception as e:

            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "w"
            ) as f:
                
                f.write(str(e))

                # stacktrace = traceback.format_exc()
                # f.write(stacktrace)

            return
        # ipdb.set_trace()

        # first, find different that are in rdf but not df with the same url

        # if not column url in rdf then continue
        if "url" not in rdf.columns or "url" not in df.columns:
            ipdb.set_trace()
            
            return

        new = rdf.loc[~rdf["url"].isin(df["url"])]

        # then find retweets with have retweetedTweet != None
        # retweets = new.loc[new["retweetedTweet"].notnull()]

        # remove retweets from new
        # new_added = new.loc[new["retweetedTweet"].isnull()]

        # append new_added to df if new_added is not empty
        # if not new_added.empty:

        #     df = df.append(new_added, ignore_index=True)

        #     df.to_parquet(tweet_path, index=False)

        # save new to parquet file
        # new.to_parquet(tweet_path, index=False)

        new.to_parquet(rt_parquet_path, index=False)

        # save csv
        # retweets.to_csv(os.path.join(env["save_path"], "users", uname, "retweets.csv"), index=False)
        # ipdb.set_trace()


fodlers = ["hashtags", "query"]

for folder in fodlers:
    save_path = os.path.join(env["save_path"], folder)
    files = os.listdir(save_path)
    total = 0
    for file in files:
        if file.endswith(".parquet"):
            df = pd.read_parquet(os.path.join(save_path, file))

            for tweet in df.itertuples():
                users = parse_tweet_users(tweet)
                print(users)

    print("Total tweets:", total)
