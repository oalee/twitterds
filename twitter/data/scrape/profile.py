import json
import traceback
import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate
import os
import dataclasses


env = yerbamate.Environment()

# from .download import download_user


def scrape_user(username):
    scraper = twitter.TwitterUserScraper(username)
    df = pd.DataFrame()
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
                df = pd.concat([df, pd.DataFrame.from_records(in_memory)])
                # df.to_parquet(tweet_path)
                in_memory = []
                # df = pd.DataFrame()

        df = pd.concat([df, pd.DataFrame.from_records(in_memory)])
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass
    
    return df

def scrape_do(username):
    scraper = twitter.TwitterProfileScraper(username)

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
                rdf = pd.concat(
                    [rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)
                # df.to_parquet(tweet_path)
                in_memory = []
                # df = pd.DataFrame()

        rdf = pd.concat(
            [rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)
    
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    return rdf
        
        
    

def scrape_tweets(username):

    # ipdb.set_trace()

    uname = username

    # make a folder with this username

    save_path = os.path.join(env["save_path"], "users", uname)
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    tweet_path = os.path.join(
        env["save_path"], "users", uname, "tweets.parquet")

    rt_parquet_path = os.path.join(
        env["save_path"], "users", uname, "retweets.parquet")

    # touch a file for indicating downloading,

    downloading_in_progress = os.path.join(
        env["save_path"], "users", uname, "downloading_in_progress.txt")

    if os.path.exists(downloading_in_progress):
        print("Already downloading:", uname)
        return

    # if empty then delete
    if os.path.exists(tweet_path):
        if os.stat(tweet_path).st_size == 0:
            os.remove(tweet_path)
        # else:
            # df = pd.read_parquet(tweet_path)
            # # if emty then delete
            # if df.empty:
            #     os.remove(tweet_path)

    if not os.path.exists(tweet_path):
        # touch the file
        open(tweet_path, "a").close()

        # touch downloading_in_progress
        open(downloading_in_progress, "a").close()

        df = pd.DataFrame()

        scraper = twitter.TwitterUserScraper(uname)

        in_memory = []

        print("Downloading tweets for:", uname, "")

        # ipdb.set_trace()

        for i, tweet in enumerate(scraper.get_items()):
            try:
                print(i, tweet.url)
                di = dataclasses.asdict(tweet)
                in_memory.append(di)
            except:
                # ipdb.set_trace()
                pass

            # save every 1000 tweets
            if len(in_memory) > 1000:
                df = pd.concat([df, pd.DataFrame.from_records(in_memory)])
                # df.to_parquet(tweet_path)
                in_memory = []
                # df = pd.DataFrame()

        df = pd.concat([df, pd.DataFrame.from_records(in_memory)])

        if not df.empty:
            df.to_parquet(tweet_path)
        # df.to_csv(os.path.join(env["save_path"], "users", uname, "tweets.csv"))

        # if os.path.exists(downloading_in_progress): os.remove(downloading_in_progress)

    else:

        rt_parquet_path = os.path.join(
            env["save_path"], "users", uname, "retweets.parquet"
        )
        # if rt_parquet_path exists then return
        if os.path.exists(rt_parquet_path) and os.stat(rt_parquet_path).st_size > 0:
            return

        df = pd.read_parquet(tweet_path)

        # if df is empty delete tweet_path and return
        if df.empty and os.path.exists(tweet_path):
            os.remove(tweet_path)

    # now, rw.parquet the file

    if not os.path.exists(rt_parquet_path) or os.stat(rt_parquet_path).st_size == 0:
        open(downloading_in_progress, "a").close()

        scraper = twitter.TwitterProfileScraper(uname)

        open(rt_parquet_path, "a").close()

        print("Downloading retweets for:", uname, "")

        # downloading_in_progress = os.path.join(

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
                    rdf = pd.concat(
                        [rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)
                    # df.to_parquet(tweet_path)
                    in_memory = []
                    # df = pd.DataFrame()

            rdf = pd.concat(
                [rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)

        except Exception as e:

            with open(
                os.path.join(env["save_path"], "users",
                             uname, "error.txt"), "w"
            ) as f:

                f.write(str(e))

                stacktrace = traceback.format_exc()
                f.write(stacktrace)

                f.write("---------------------------\n")

            if os.path.exists(downloading_in_progress):
                os.remove(downloading_in_progress)
            # os.remove(downloading_in_progress)
            rdf = pd.concat(
                [rdf, pd.DataFrame.from_records(in_memory)], ignore_index=True)

        # ipdb.set_trace()

        # first, find different that are in rdf but not df with the same url


        # if both df and rdf are empty then delete the file
        if df.empty:
            if os.path.exists(tweet_path) and os.stat(tweet_path).st_size == 0:
                os.remove(tweet_path)

            # os.remove(tweet_path)

            if not rdf.empty:
                rdf.to_parquet(rt_parquet_path, index=False)

            return

        # if not column url in rdf then continue

        if "url" not in rdf.columns or "url" not in df.columns:
            # ipdb.set_trace()
            # save error
            with open(
                os.path.join(env["save_path"], "users",
                             uname, "error.txt"), "w"
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

    #     if uname == "Nasim__iran":
    #         ipdb.set_trace()
    # #

        # new = rdf.loc[~rdf["url"].isin(df["url"])]

        rdf.to_parquet(rt_parquet_path, index=False)


        # if os.path.exists(downloading_in_progress):
        #     os.remove(downloading_in_progress)
        # delete the downloading_in_progress file


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
        scrape_tweets(env.uname)
    else:
        get_profiles_from_os_list()
