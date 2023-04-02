import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses


start_date = "2022-08-01"
end_date = "2023-05-01"
env = yerbamate.Environment()

hashtags = ["ننگ_بر_فتنه_۵۷", "مرگ_بر_سه_فاسد_ملا_چپی_مجاهد" , "مرد_میهن_آبادی", "زن_زندگی_آزادی"]


download_hashtags = [
    "رضا_شاه_روحت_شاد", "رضاشاه_روحت_شاد" , "جاویدشاه"
]

def download_hashtag_tweets(
    hashtag, start_date, end_date, mode=twitter.TwitterSearchScraperMode.TOP
):

    save_path = os.path.join(
        env["save_path"], "hashtags", f"{hashtag}_{start_date}_{end_date}_{mode.value}.parquet"
    )

    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    df = pd.DataFrame()

    scraper = twitter.TwitterSearchScraper(
        f"#{hashtag} since:{start_date} until:{end_date}", mode=mode
    )

    # scraper is a generator, so we can iterate over it

    in_memory = []

    for i, tweet in enumerate(scraper.get_items()):
        print(i, tweet.url)
        di = dataclasses.asdict(tweet)
        # use pandas.concat to append the new tweet to the dataframe

        in_memory.append(di)
        # df = pd.concat([df, di] , ignore_index=True)
        # df = df.append(di, ignore_index=True)

        # persist the dataframe every 1000 tweets
        if i % 1000 == 0:
            df = pd.concat([df, pd.DataFrame(in_memory)], ignore_index=True)
            df.to_parquet(save_path)
            in_memory = []

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
        download_hashtag_tweets(hashtag, fmt_month, fmt_next_month, mode=twitter.TwitterSearchScraperMode.TOP)
        download_hashtag_tweets(hashtag, fmt_month, fmt_next_month, mode=twitter.TwitterSearchScraperMode.LIVE)
        month = next_month

# download_increasing_months(hashtags[3], start_date, end_date)

for hashtags in download_hashtags:
    download_increasing_months(hashtags, start_date, end_date)
