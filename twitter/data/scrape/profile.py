import json
import traceback
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

        print("Downloading tweets for:", uname, "")

        for i, tweet in enumerate(scraper.get_items()):
            try:
                print(i, tweet.url)
            except:
                pass
            di = dataclasses.asdict(tweet)
            in_memory.append(di)

            # save every 1000 tweets
            if len(in_memory) > 1000:
                # df = df.append(in_memory, ignore_index=True)
                df = pd.concat([df, pd.DataFrame.from_records(in_memory)])
                # df.to_parquet(tweet_path)
                in_memory = []
                # df = pd.DataFrame()

        df = pd.concat([df, pd.DataFrame.from_records(in_memory)])
        # df = df.append(in_memory, ignore_index=True)

        df.to_parquet(tweet_path)
        # df.to_csv(os.path.join(env["save_path"], "users", uname, "tweets.csv"))

    else:
        # if empty then return
        if os.stat(tweet_path).st_size == 0:
            print("Empty file:", tweet_path)
            # save error
            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "a+"
            ) as f:
                # append error

                f.write("\nEmpty file:" + tweet_path)
            return

        df = pd.read_parquet(tweet_path)

    # now, rw.parquet the file
    rt_parquet_path = os.path.join(env["save_path"], "users", uname, "retweets.parquet")
    if not os.path.exists(rt_parquet_path):
        scraper = twitter.TwitterProfileScraper(uname)

        open(rt_parquet_path, "a").close()

        print("Downloading retweets for:", uname, "")

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
                    # rdf = rdf.append(in_memory, ignore_index=True)
                    rdf = pd.concat([rdf, pd.DataFrame.from_records(in_memory)])
                    # df.to_parquet(tweet_path)
                    in_memory = []
                    # df = pd.DataFrame()
            
            rdf = pd.concat([rdf, pd.DataFrame.from_records(in_memory)])

            # rdf = rdf.append(in_memory, ignore_index=True)

        except Exception as e:

            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "a+"
            ) as f:

                f.write(str(e))

                stacktrace = traceback.format_exc()
                f.write(stacktrace)

            return
        # ipdb.set_trace()

        # first, find different that are in rdf but not df with the same url

        # if not column url in rdf then continue
        if "url" not in rdf.columns or "url" not in df.columns:
            # ipdb.set_trace()
            # save error
            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "w"
            ) as f:
                # append error

                f.write(
                    "url not in rdf or df, probably protected/private account, skipping"
                )
                # dump json of tweet
                # f.write(json.dumps(tweet))

                # print("DF, PATH", tweet_path)
            return

        new = rdf.loc[~rdf["url"].isin(df["url"])]

        new.to_parquet(rt_parquet_path, index=False)


def scrape_tweets(username):

    # ipdb.set_trace()

    uname = username

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

        in_memory = []

        print("Downloading tweets for:", uname, "")

        for i, tweet in enumerate(scraper.get_items()):
            try:
                print(i, tweet.url)
                di = dataclasses.asdict(tweet)
                in_memory.append(di)
            except:
                pass

            # save every 1000 tweets
            if len(in_memory) > 1000:
                df = pd.concat([df, pd.DataFrame.from_records(in_memory)])
                # df.to_parquet(tweet_path)
                in_memory = []
                # df = pd.DataFrame()

        df = pd.concat([df, pd.DataFrame.from_records(in_memory)])

        df.to_parquet(tweet_path)
        # df.to_csv(os.path.join(env["save_path"], "users", uname, "tweets.csv"))

    else:
        # if empty then return
        if os.stat(tweet_path).st_size == 0:
            print("Empty file:", tweet_path)
            # save error
            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "w"
            ) as f:
                # append error

                f.write("\nEmpty file:" + tweet_path)
                f.write("\nProbably a protected account, skipping")
                f.write("------------------\n")

            return
        rt_parquet_path = os.path.join(
            env["save_path"], "users", uname, "retweets.parquet"
        )
        # if rt_parquet_path exists then return
        if os.path.exists(rt_parquet_path):
            return
        df = pd.read_parquet(tweet_path)

    # now, rw.parquet the file
    rt_parquet_path = os.path.join(env["save_path"], "users", uname, "retweets.parquet")
    if not os.path.exists(rt_parquet_path):
        scraper = twitter.TwitterProfileScraper(uname)

        open(rt_parquet_path, "a").close()

        print("Downloading retweets for:", uname, "")

        rdf = pd.DataFrame()

        in_memory = []

        try:
            for i, tweet in enumerate(scraper.get_items()):
                try:
                    print(i, tweet.url)
                    di = dataclasses.asdict(tweet)
                    in_memory.append(di)
                except:
                    pass
                # save every 1000 tweets
                if len(in_memory) > 1000:
                    rdf = pd.concat([rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)
                    # df.to_parquet(tweet_path)
                    in_memory = []
                    # df = pd.DataFrame()

            rdf = pd.concat([rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)

        except Exception as e:

            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "w"
            ) as f:

                f.write(str(e))

                stacktrace = traceback.format_exc()
                f.write(stacktrace)

                f.write("---------------------------\n")

            return
        # ipdb.set_trace()

        # first, find different that are in rdf but not df with the same url

        # if not column url in rdf then continue
        if "url" not in rdf.columns or "url" not in df.columns:
            # ipdb.set_trace()
            # save error
            with open(
                os.path.join(env["save_path"], "users", uname, "error.txt"), "w"
            ) as f:
                # append error

                f.write(
                    "url not in rdf or df, probably protected/private account, skipping"
                )

                f.write("---------------------------\n")
                # dump json of tweet
                # f.write(json.dumps(tweet))

                # print("DF, PATH", tweet_path)
            return

        new = rdf.loc[~rdf["url"].isin(df["url"])]

        new.to_parquet(rt_parquet_path, index=False)


# fodlers = ["hashtags", "query"]

# for folder in fodlers:
#     save_path = os.path.join(env["save_path"], folder)
#     files = os.listdir(save_path)
#     total = 0
#     for file in files:
#         if file.endswith(".parquet"):
#             df = pd.read_parquet(os.path.join(save_path, file))

#             for tweet in df.itertuples():
#                 users = parse_tweet_users(tweet)
#                 # print(users)

#     print("Total tweets:", total)


def get_profiles_from_os_list():

    save_path = os.path.join(env["save_path"], "users")
    files = os.listdir(save_path)
    total = 0
    print("Total users:", len(files))
    for file in files:
        try:
            scrape_tweets(file)
        except Exception as e:
            print(e)
            print("Error with:", file)
            # save error
            with open(os.path.join(save_path, file, "error.txt"), "a+") as f:
                # append error

                f.write(str(e))

                stacktrace = traceback.format_exc()
                f.write(stacktrace)

                f.write("---------------------------\n")
            continue


if __name__ == "__main__":

    if hasattr(env, "uname"):
        scrape_tweets(env["uname"])
    else:
        get_profiles_from_os_list()
