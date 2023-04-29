import os
import pandas as pd
from yerbamate import Environment
from ..loader.prepro import get_userlist, get_user, clean_text
import tqdm

env = Environment()



def clean_content(content):
    if not isinstance(content, str):
        content = str(content)
    return clean_text(content)


def id_prepro(user_df):
    if user_df.empty:
        return user_df

    user_df['month_year'] = user_df['date'].dt.strftime('%Y-%m')
    user_df['text'] = user_df['rawContent'].apply(clean_content)

    user_df = user_df[user_df['user'].notnull()]

    user_df['userId'] = user_df['user'].apply(lambda x: int(x['id']) if x.get('id') is not None else None)

    remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates',
                      'source', 'sourceUrl', 'sourceLabel', 'renderedContent', 'links', 'textLinks']
    user_df = user_df.drop(columns=remove_columns, errors='ignore')

    user_df['media'] = user_df['media'].notnull()

    def get_id(x):
        return int(x['id']) if pd.notna(x) and x.get('id') is not None else None

    user_df['retweetedTweetId'] = user_df['retweetedTweet'].apply(get_id)
    user_df['retweetedUserId'] = user_df['retweetedTweet'].apply(lambda x: int(x['user']['id']) if pd.notna(x) and x.get('user') is not None and x['user'].get('id') is not None else None)
    user_df['quotedTweetId'] = user_df['quotedTweet'].apply(get_id)
    user_df['inReplyToUserId'] = user_df['inReplyToUser'].apply(get_id)

    user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
        lambda x: [int(user['id']) for user in x] if x is pd.notna(x) and len(x) > 0 else [])

    user_df['hashtags'] = user_df['hashtags'].apply(lambda x: x if x is not None else [])

    return user_df


def append_to_parquet_file(input_file, new_data):
    if new_data.empty:
        return
    if os.path.exists(input_file):
        existing_data = pd.read_parquet(input_file, engine="pyarrow")
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=['id'])
    else:
        combined_data = new_data

    combined_data.to_parquet(input_file, engine="pyarrow")

def save_data_to_parquet(grouped_data, file_prefix):
    for month_year, group in grouped_data:
        output_dir = os.path.join(env['sv_path'], 'time')
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f'{month_year}-{file_prefix}.parquet'
        output_path = os.path.join(output_dir, output_filename)

        append_to_parquet_file(output_path, group)

def process_batch(tweets_batch, retweets_batch):
    tweets_df = pd.concat(tweets_batch, ignore_index=True)
    retweets_df = pd.concat(retweets_batch, ignore_index=True)

    save_data_to_parquet(tweets_df.groupby('month_year'), 'tweets')
    save_data_to_parquet(retweets_df.groupby('month_year'), 'retweets')

def extract():
    users_list = get_userlist()
    batch_size = 10  # Adjust this value based on your system's memory constraints

    tweets_batch = []
    retweets_batch = []

    for idx, username in enumerate(tqdm.tqdm(users_list, total=len(users_list))):
        user_df = get_user(username)

        if user_df is None or user_df.empty:
            continue

        user_df = id_prepro(user_df)  # preprocess tweets and add ids

        # Split into tweets and retweets
        retweets = user_df[user_df['retweetedTweetId'].notnull()]
        tweets = user_df[user_df['retweetedTweetId'].isnull()]

        # Accumulate tweets and retweets in their respective lists
        tweets_batch.append(tweets)
        retweets_batch.append(retweets)

        if (idx + 1) % batch_size == 0:
            process_batch(tweets_batch, retweets_batch)
            tweets_batch = []
            retweets_batch = []

    # Process the remaining data
    if tweets_batch or retweets_batch:
        process_batch(tweets_batch, retweets_batch)

if __name__ == "__main__":
    extract()
