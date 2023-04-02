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

        in_memory = []
        for i, tweet in enumerate(scraper.get_items()):
            print(i, tweet.url)
            di = dataclasses.asdict(tweet)
            in_memory.append(di)

 
 
        df = df.append(in_memory, ignore_index=True)

        df.to_parquet(tweet_path)
        # df.to_csv(os.path.join(env["save_path"], "users", uname, "tweets.csv"))

    else:
        pass


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
