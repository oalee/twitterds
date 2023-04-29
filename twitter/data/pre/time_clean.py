import os
import pandas as pd
from yerbamate import Environment
from ..loader.prepro import get_userlist, get_user, clean_text
import tqdm
import ipdb

env = Environment()

def user_generator(users_list):
    for user in users_list:
        user_df = get_user(user)
        if user_df is not None:
            yield user_df


def clean_content(content):
    if not isinstance(content, str):
        content = str(content)
    return clean_text(content)


def append_to_parquet_file(input_file, new_data):
    if new_data.empty:
        return
    if os.path.exists(input_file):
        existing_data = pd.read_parquet(input_file, engine="pyarrow")
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=['id'])
    else:
        combined_data = new_data
    
    try:
        combined_data.to_parquet(input_file, engine="pyarrow")
    except:
        ipdb.set_trace()


def create_global_profile(user_list, output_dir):

    user_profile_columns = ['username', 'preprocessed', 'error', 'created', 'descriptionLinks', 'displayname', 'favouritesCount', 'followersCount', 'friendsCount',
                            'id', 'listedCount', 'location', 'mediaCount', 'protected', 'rawDescription', 'statusesCount', 'verified']
    # combines profiles
    global_profile = pd.DataFrame(columns=user_profile_columns,
                                  data=[[user, False, False, None, None, None, None, None, None, None, None, None, None, None, None, None, None] for user in user_list])

#  set index to username
    global_profile.set_index('username', inplace=True)
    # global_profile = pd.DataFrame(columns=['username', 'preprocessed', 'error'], data=[
    #     [user, False, False] for user in user_list])
    global_profile.to_parquet(os.path.join(
        output_dir, 'global_profile.parquet'), engine="pyarrow")

    return global_profile


def update_global_profile_status(global_profile, output_dir, username, preprocessed, error):

    global_profile.loc[username, ['preprocessed', 'error']] = [
        preprocessed, error]
    # global_profile.to_parquet(os.path.join(output_dir, 'global_profile.parquet'), engine="pyarrow")


def add_profiles(global_profile, new_profiles):
    # add columns if not exist for preprocessed and error
    if 'preprocessed' not in new_profiles.columns:

        new_profiles['preprocessed'] = False
    if 'error' not in new_profiles.columns:
        new_profiles['error'] = False

    # Set the index of both dataframes to 'id'
    # global_profile.set_index('username', inplace=True)
    new_profiles.set_index('username', inplace=True)

    # Update the global_profile with new_profile data
    global_profile.update(new_profiles)
    return global_profile


def add_to_profile(global_profile, new_profile):
    # add columns if not exist for preprocessed and error
    if 'preprocessed' not in global_profile.columns:
        global_profile['preprocessed'] = False
    if 'error' not in global_profile.columns:
        global_profile['error'] = False

    # Set the index of both dataframes to 'id'
    global_profile.set_index('username', inplace=True)
    new_profile.set_index('username', inplace=True)

    # Update the global_profile with new_profile data
    global_profile.update(new_profile)

    return global_profile


def save_user_profiles(user_df, output_dir):
    profiles = user_df['user'].apply(pd.Series).drop_duplicates(subset=['id'])
    user_profile_columns = ['created', 'descriptionLinks', 'displayname', 'favouritesCount', 'followersCount', 'friendsCount',
                            'id', 'listedCount', 'location', 'mediaCount', 'protected', 'rawDescription', 'statusesCount', 'username', 'verified']
    profiles = profiles[user_profile_columns]
    profiles.to_parquet(os.path.join(
        output_dir, 'user_profiles.parquet'), engine="pyarrow")


def id_prepro(user_df):

    if user_df.empty:
        return user_df
    user_df['month_year'] = user_df['date'].apply(
        lambda x: x.strftime('%Y-%m'))



    # apply clean_content function
    user_df['text'] = user_df['rawContent'].apply(clean_content)

    none_user_indices = user_df[user_df['user'].isnull()].index
    user_df.drop(none_user_indices, inplace=True)

    user_df['userId'] = user_df['user'].apply(
        lambda x: x['id'] if type(x) is not None else None)

    
    remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates',
                      'source', 'sourceUrl', 'sourceLabel', 'renderedContent', 'links', 'textLinks']

    user_df = user_df.drop(columns=remove_columns, errors='ignore')

    # change media to boolean if not null
    user_df['media'] = user_df['media'].apply(
        lambda x: True if x is not None else False)

    # Convert user, retweetedTweet, quotedTweet, inReplyToUser, and mentionedUsers to their respective IDs
    user_df['retweetedTweetId'] = user_df['retweetedTweet'].apply(
        lambda x: x['id'] if pd.notna(x) else None)
    user_df['quotedTweetId'] = user_df['quotedTweet'].apply(
        lambda x: x['id'] if pd.notna(x) else None)
    user_df['inReplyToUserId'] = user_df['inReplyToUser'].apply(
        lambda x: x['id'] if pd.notna(x) else None)
    user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
        lambda x: [user['id'] for user in x] if x is pd.notna(x) and len(x) > 0 else [])

    user_df['hashtags'] = user_df['hashtags'].apply(
        lambda x: x if x is not None else [])

    return user_df


def drop_column_prepro(tweets):
    return tweets.drop(
        columns=['retweetedTweet', 'quotedTweet', 'inReplyToUser', 'mentionedUsers', 'user'])


def preprocess_tweets(global_profile, user_df, output_dir):

    
    user_df = id_prepro(user_df)  # preprocess tweets and add ids

    retweets = user_df[user_df['retweetedTweet'].notnull()
                       ]['retweetedTweet'].apply(pd.Series)  # get retweets
    
    retweets = id_prepro(retweets)  # preprocess retweets and add ids

    q_df = user_df[user_df['quotedTweet'].notnull()]['quotedTweet'].apply(pd.Series)  # get quoted tweets
    
    try:
        q_df = id_prepro(q_df)
    except:

        q_df = pd.DataFrame()

    tweets = pd.concat([user_df, q_df, retweets], ignore_index=True)

    users = tweets['user'].apply(pd.Series).drop_duplicates(subset=['id'])

  
    add_profiles(global_profile, users)
  
  
    tweets = drop_column_prepro(tweets)

    grouped_tweets = tweets.groupby('month_year')
    
    
    for month_year, group in grouped_tweets:
        output_dir = os.path.join(env['sv_path'], 'time')
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f'{month_year}-tweets.parquet'
        output_path = os.path.join(output_dir, output_filename)


        if 0 in group.columns:
            group = group.drop(columns=[0])
            # ipdb.set_trace()
        append_to_parquet_file(output_path, group)



def extract():

    # ipdb.set_trace()
    users_list = get_userlist()

    output_dir = os.path.join(env['sv_path'], 'profiles')
    os.makedirs(output_dir, exist_ok=True)

    # Read global profile if it exists, or create an empty one
    global_profile_path = os.path.join(output_dir, 'global_profile.parquet')
    if os.path.exists(global_profile_path):
        global_profile = pd.read_parquet(global_profile_path, engine="pyarrow")
        # global_profile.set_index('username', inplace=True)
    else:
        global_profile = create_global_profile(users_list, output_dir)

    users_processed = 0
    update_interval = 50

    for username in tqdm.tqdm(users_list, total=len(users_list)):

        # Skip users that have already been processed
        # ipdb.set_trace()
        user_preprocessed = global_profile.loc[username, 'preprocessed']

        if user_preprocessed:
            continue

        user_df = get_user(username)

        if user_df is None or user_df.empty:
            update_global_profile_status(
                global_profile, output_dir, username, True, True)
            users_processed += 1
        else:
            # Save user profiles
            # save_user_profiles(user_df, output_dir)

            # Preprocess tweets
            preprocess_tweets(global_profile, user_df, output_dir)

            # Update global profile
            # ipdb.set_trace()
            update_global_profile_status(
                global_profile, output_dir, username, True, False)
            users_processed += 1

        # Update global profile every 100 users (or any other value set in update_interval)
        if users_processed % update_interval == 0:
            global_profile.to_parquet(os.path.join(
                output_dir, 'global_profile.parquet'), engine="pyarrow")

    # Save the final global profile after processing all users
    global_profile.to_parquet(os.path.join(
        output_dir, 'global_profile.parquet'), engine="pyarrow")


if __name__ == "__main__":
    extract()
