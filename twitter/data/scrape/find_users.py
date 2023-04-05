import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses


start_date = "2022-08-01"
end_date = "2023-05-01"
env = yerbamate.Environment()


query = "زن زندگی آزادی"


existing_find_users = os.listdir(os.path.join(env["save_path"], "users"))


def add_users(users):
    for user in users:
        add_user(user)


def add_user(username):
    if username not in existing_find_users:
        existing_find_users.append(username)
        os.mkdir(os.path.join(env["save_path"], "users", username), exist_ok=True)
        print("Added user:", username)


def download_tweets(
    query, start_date, end_date, mode=twitter.TwitterSearchScraperMode.TOP
):

    save_path = os.path.join(
        env["save_path"],
        "query",
        f"{query}_{start_date}_{end_date}_{mode.value}.parquet",
    )

    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    df = pd.DataFrame()

    scraper = twitter.TwitterSearchScraper(
        f"{query} since:{start_date} until:{end_date}", mode=mode
    )

    # scraper is a generator, so we can iterate over it

    in_memory = []

    for i, tweet in enumerate(scraper.get_items()):
        try:
            print(i, tweet.url)
            di = dataclasses.asdict(tweet)
            # use pandas.concat to append the new tweet to the dataframe
            username = di["user"]["username"]
            add_user(username)
            in_memory.append(di)
            if i % 1000 == 0:
                df = pd.concat([df, pd.DataFrame(in_memory)], ignore_index=True)
                # df.to_parquet(save_path)
                in_memory = []
        except Exception as e:
            print(e)

    # persist the dataframe at the end
    if df.size > 0 or len(in_memory) > 0:
        if len(in_memory) > 0:
            df = pd.concat([df, pd.DataFrame(in_memory)], ignore_index=True)
        df.to_parquet(save_path)

    # return tweets


def download_increasing_months(hashtag, start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    month = start_date
    while month < end_date:

        next_month = month + pd.DateOffset(months=1)
        fmt_month = month.strftime("%Y-%m-%d")
        fmt_next_month = next_month.strftime("%Y-%m-%d")
        download_tweets(
            hashtag,
            fmt_month,
            fmt_next_month,
            mode=twitter.TwitterSearchScraperMode.TOP,
        )
        download_tweets(
            hashtag,
            fmt_month,
            fmt_next_month,
            mode=twitter.TwitterSearchScraperMode.LIVE,
        )
        month = next_month



download_increasing_months(query, start_date, end_date)