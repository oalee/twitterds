import os
import pandas as pd
from yerbamate import Environment
from ..loader.prepro import get_userlist, get_user, clean_text
import tqdm
import ipdb


# Define a generator function to iterate over users
def user_generator(users_list):
    for user in users_list:
        user_df = get_user(user)
        if user_df is not None:
            yield user_df

# Define a clean_content function if you don't have one


def clean_content(content):
    # Apply your cleaning logic here, for example:
    if not isinstance(content, str):
        # ipdb.set_trace()
        content = str(content)

    return clean_text(content)


def append_to_parquet_file(input_file, new_data):
    # empty check
    if new_data.empty:
        return
    if os.path.exists(input_file):
        existing_data = pd.read_parquet(input_file, engine="pyarrow")
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        # remove duplicates on id
        combined_data = combined_data.drop_duplicates(subset=['id'])
    else:
        combined_data = new_data

    combined_data.to_parquet(input_file, engine="pyarrow")


def extract():

    env = Environment()

    users_list = get_userlist()

    # somehow cache and create user profiles (with user id, name, preprocesed etc.
    # and save it to a file, so that we can use it later on)

# Process data for each user
    for username in tqdm.tqdm(users_list, total=len(users_list)):
        # Ensure 'date' column is a datetime object
        # user_df['date'] = pd.to_datetime(user_df['date'])

        # check if user already analyzed

        
        # if not, get user df
        user_df = get_user(username)

        # if none continue
        if user_df is None:
            continue
        # empty check
        if user_df.empty:
            continue
        # ipdb.set_trace()

        # Extract month-year information
        user_df['month_year'] = user_df['date'].apply(
            lambda x: x.strftime('%Y-%m'))

        # apply clean_content function
        user_df['text'] = user_df['rawContent'].apply(clean_content)

        user_df['userId'] = user_df['user'].apply(
            lambda x: x['id'] if x is not None else None)

        none_user_indices = user_df[user_df['user'].isnull()].index
        user_df.drop(none_user_indices, inplace=True)

        # Keep the required columns and remove unwanted columns
        required_columns = ['url', 'id', 'rawContent', 'text', 'user',
                            'retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers']
        remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates',
                          'source', 'sourceUrl', 'sourceLabel', 'renderedContent', 'user', 'media', 'links', 'textLinks']

        user_df = user_df.drop(columns=remove_columns, errors='ignore')

        # Convert user, retweetedTweet, quotedTweet, inReplyToUser, and mentionedUsers to their respective IDs
        user_df['retweetedTweetId'] = user_df['retweetedTweet'].apply(
            lambda x: x['id'] if x else None)
        user_df['quotedTweetId'] = user_df['quotedTweet'].apply(
            lambda x: x['id'] if x else None)
        user_df['inReplyToUserId'] = user_df['inReplyToUser'].apply(
            lambda x: x['id'] if x else None)
        user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
            lambda x: [user['id'] for user in x] if x is not None and len(x) > 0 else [])

        user_df['hashtags'] = user_df['hashtags'].apply(
            lambda x: x if x is not None else [])
        
        # extract rewteet and quoted tweet, (only where not null)
        retweets = user_df[user_df['retweetedTweet'].notnull()]
        # retweets_o



        quoted_df = user_df[user_df['quotedTweet'].notnull()]['quotedTweet']

        user_df = user_df.drop(
            columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers'])


        tweets = pd.concat([user_df, quoted_df], ignore_index=True)

    

        # save retweets and quoted tweets

        # Group tweets and retweets by month_year
        grouped_tweets = tweets.groupby('month_year')
        grouped_retweets = retweets.groupby('month_year')
        # Save the grouped tweets and retweets into separate parquet files
        # ipdb.set_trace()

        for month_year, group in grouped_tweets:
            output_dir = os.path.join(env['sv_path'], 'time')
            os.makedirs(output_dir, exist_ok=True)

            output_filename = f'{month_year}-tweets.parquet'
            output_path = os.path.join(output_dir, output_filename)

            append_to_parquet_file(output_path, group)

        for month_year, group in grouped_retweets:
            output_dir = os.path.join(env['sv_path'], 'time')
            os.makedirs(output_dir, exist_ok=True)

            output_filename = f'{month_year}-retweets.parquet'

            append_to_parquet_file(output_path, group)


if __name__ == "__main__":
    extract()
