
import numpy as np
import pandas as pd
import json


def safe_str_int(value):
    if value is None:
        return None
    try:
        return np.str0(np.int64(value))
    except (TypeError, ValueError):
        return None


def safe_int(value):
    if value is None:
        return None
    try:
        return np.int64(value)
    except (TypeError, ValueError):
        return None


def id_prepro(user_df):
    if user_df.empty:
        return user_df

    user_df['month_year'] = user_df['date'].dt.strftime('%Y-%m')
    # user_df['text'] = user_df['rawContent'].apply(clean_content)

    user_df = user_df[user_df['user'].notnull()].copy()

    remove_columns = ['vibe', 'place', 'card', 'cashtags', 'coordinates', 'text', 'url',
                      'source', 'sourceUrl', 'sourceLabel', 'renderedContent', 'links', 'textLinks']

    user_df = user_df.drop(columns=remove_columns, errors='ignore')

    user_df['media'] = user_df['media'].notnull()

    user_df['userId'] = user_df['user'].apply(
        lambda x: safe_str_int(x['id']) if x.get('id') is not None else None)

    def get_id(x):
        return safe_str_int(x['id']) if pd.notna(x) and x.get('id') is not None else None

    user_df['retweetedTweetId'] = user_df['retweetedTweet'].apply(get_id)
    user_df['retweetedUserId'] = user_df['retweetedTweet'].apply(lambda x: safe_str_int(x['user']['id']) if pd.notna(
        x) and x.get('user') is not None and x['user'].get('id') is not None else None)
    user_df['quotedTweetId'] = user_df['quotedTweet'].apply(get_id)
    user_df['inReplyToUserId'] = user_df['inReplyToUser'].apply(get_id)

    # user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(lambda x: [safe_int(user['id']) for user in x] if pd.notnull(x) else None)

    user_df['mentionedUserIds'] = user_df['mentionedUsers'].apply(
        lambda x: json.dumps([user['id'] for user in x]) if x is not None else None)

    user_df['hashtags'] = user_df['hashtags'].apply(
        lambda x: json.dumps([item for item in x]) if x is not None else None)

    for col in ['userId', 'retweetedTweetId', 'conversationId', 'retweetedUserId', 'quotedTweetId', 'inReplyToUserId', 'inReplyToUserId', 'inReplyToTweetId']:
        user_df[col] = user_df[col].apply(
            lambda x: safe_str_int(x) if pd.notna(x) else None)

    # convert counts to int
    columns = ['replyCount', 'retweetCount',
               'likeCount', 'quoteCount', 'viewCount']

    for col in columns:
        user_df[col] = user_df[col].apply(safe_str_int)

    return user_df

