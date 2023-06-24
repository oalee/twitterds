import yerbamate, os
import pandas as pd

env = yerbamate.Environment()

root_data_path = env["data"]


def get_tweets_user(username):
    path = os.path.join(root_data_path, "users", username, "tweets.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} does not exist")

    return pd.read_parquet(
        path
    )
