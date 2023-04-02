import os, pandas as pd, yerbamate, ipdb, dataclasses, snscrape.modules.twitter as twitter

env = yerbamate.Environment()


# users path
user_path = os.path.join(env["save_path"], "users")


class UserProfileNode:
    def __init__(self, username):
        self.username = username
        self.save_path = os.path.join(user_path, username)
        self.tweets_path = os.path.join(self.save_path, "tweets.parquet")
        self.tweets = None
        self.followers = None

    def load_tweets(self):

        self.tweets = pd.read_parquet(self.tweets_path)

        self.user = self.tweets.iloc[0].user

        self.tweets = self.tweets.drop(columns=["user"])
        self.scrape_profile()
        return self.tweets

    def scrape_profile(self):
        scraper = twitter.TwitterProfileScraper(self.user["username"])

        in_memory = []
        for i, tweet in enumerate(scraper.get_items()):
            print(i, tweet.url)
            di = dataclasses.asdict(tweet)
            
            in_memory.append(di)
        pass
        
        ipdb.set_trace()


for user in os.listdir(user_path):

    # if user.startswith("."):
    #     continue

    print(f"Loading user {user}")
    user_node = UserProfileNode(user)
    user_node.load_tweets()

    ipdb.set_trace()
