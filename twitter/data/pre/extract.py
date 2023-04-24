import os
import tqdm
import yerbamate
import pandas as pd
import re
import ipdb

env = yerbamate.Environment()


def clean_text(text):
    if text is None:
        return None
    # Remove mentions
    # try:
    #     text = re.sub(r'@\w+', '', text)
    # except:
    #     ipdb.set_trace()
    text = re.sub(r'@\w+', '', text)

    # Remove URLs
    text = re.sub(r'http\S+|https\S+', 'URL$', text)

    # Remove extra whitespaces
    # try:
    #     text = re.sub(r'\s+', ' ', text).strip()
    # except:
    #     ipdb.set_trace()

    # Remove "RT :"
    text = '' if text.startswith('RT :') else text

    return text


def get_user_tweets(user_name):

    path = os.path.join(env["td_path"], "users", user_name)
    df = None
    rdf = None
    # check if tweets exist
    if os.path.exists(os.path.join(path, "tweets.parquet")):
        # check if not empty

        try:
            df = pd.read_parquet(os.path.join(path, "tweets.parquet"))
        except:
            df = None

    if os.path.exists(os.path.join(path, "retweets.parquet")):
        # check if not empty
        try:
            rdf = pd.read_parquet(os.path.join(path, "retweets.parquet"))
        except:
            rdf = None

    if df is not None and rdf is not None:
        df = pd.concat([df, rdf], ignore_index=True)
    elif df is None and rdf is not None:
        df = rdf

    # remove RTs
    # if df is not None:
    #     df = df[~df['rawContent'].str.startswith('RT')]

    # only keep rawContent
    if df is not None and "rawContent" in df.columns:
        df = df[["rawContent"]]
        df = df.rename(columns={"rawContent": "text"})
        try:
            df["text"] = df["text"].apply(clean_text)
        except:
            ipdb.set_trace()
        df = df[df["text"] != ""]
        df = df.drop_duplicates()
        df = df.reset_index(drop=True)

    return df


def generate_tweets():

    users = os.listdir(os.path.join(env["td_path"], "users"))
    # tqdm.tqdm(users)
    for user in tqdm.tqdm(users):

        file = os.path.join(env["data"], "tweets", f"{user}.parquet")

        if os.path.exists(file):
            if os.stat(file).st_size != 0:
                continue

        df = get_user_tweets(user)
        if df is not None:
            os.makedirs(os.path.dirname(file), exist_ok=True)
            df.to_parquet(os.path.join(
                env["data"], "tweets", f"{user}.parquet"))
        # ipdb.set_trace()


if __name__ == "__main__":
    generate_tweets()
