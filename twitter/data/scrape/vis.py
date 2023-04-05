import json
import sys
import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses, tqdm

import numpy as np

def numpy_to_python(obj):
    if isinstance(obj, dict):
        return {k: numpy_to_python(v) for k, v in obj.items()}
    elif isinstance(obj, (np.ndarray, np.generic)):
        return obj.tolist()
    # Convert numpy number to native Python number
    elif isinstance(obj, np.number):
        return obj.item()
    # Convert lists recursively
    elif isinstance(obj, list):
        return [numpy_to_python(item) for item in obj]
    else:
        return obj



env = yerbamate.Environment()


# ipdb.set_trace()
save_path = os.path.join(env["save_path"], "users")

files = os.listdir(save_path)
total = 0
cnt = 0

# handle interrupt

users = []

for dir in tqdm.tqdm(files):
    # ipdb.set_trace()
    # ipdb.set_trace()

    #   check if metadata exists
    if os.path.exists(os.path.join(env["save_path"], "users",dir, "metadata.json")):
        try:
            metadata = json.load(open(os.path.join(env["save_path"], "users", dir, "metadata.json")))
        except:
            # remove file
            os.remove(os.path.join(env["save_path"], "users",dir, "metadata.json"))
            metadata = None

    tweets_path = os.path.join(env["save_path"], "users", dir, "tweets.parquet")
    retweets_path = os.path.join(env["save_path"], "users", dir, "retweets.parquet")

    if os.path.exists(tweets_path):

        last_modified = os.path.getmtime(tweets_path)

        if metadata != None and metadata["date"] >= last_modified:
            # print("Skipping", dir, "because it is already up to date.")

            # check if retweets are up to date
            if os.path.exists(retweets_path):
                last_modified = os.path.getmtime(retweets_path)
                if metadata["date"] >= last_modified:
                    # print("Skipping", dir, "because it is already up to date.")
                    users.append(metadata)
                    continue

        # if empty, continue
        if os.stat(tweets_path).st_size == 0:
            continue

        try:
            df = pd.read_parquet(tweets_path)
            user = df["user"][0]

            # if type series, duplicates are there, so remove them
            if type(user) == pd.core.series.Series:
                user = user.drop_duplicates().iloc[0]
        except:
            # ipdb.set_trace()
            # probably empty
            continue
        # get total likes
        likes = df["likeCount"].sum()
        # get total retweets
        # change to python number
        # ipdb.set_trace()
        # likes = likes

        tweet_count = df.shape[0]

        if os.path.exists(retweets_path):

            if os.stat(retweets_path).st_size == 0:

                retweets = 0
                # continue
            else:
                df = pd.read_parquet(retweets_path)
                retweets = df.shape[0]

        else:
            retweets = 0

        metadata = {
            "tweets_count": tweet_count,
            "retweets_count": retweets,
            "likes_count": likes.item(),
            "date": last_modified,
            "user": user,
        }

        metadata["user"]["created"] = metadata["user"]["created"].strftime(
                "%Y-%m-%dT%H:%M:%S%z"
            )
        
        # ipdb.set_trace()

        metadata = numpy_to_python(metadata)

        # ipdb.set_trace()
        # if link in metadata["user"]["link"]:, link['indices'] = are np.array, convert to list

        try:

            json.dump(metadata, open(os.path.join(save_path, dir, "metadata.json"), "w"))
        except:
            # ipdb.set_trace()
            continue

        users += [metadata]


# ipdb.set_trace()

# sum over tweets_count, retweets_count

tweets_count = sum([user["tweets_count"] for user in users])
retweets_count = sum([user["retweets_count"] for user in users])


print("Users:", len(users))
print("Tweets:", tweets_count)
print("Retweets:", retweets_count)
# df = pd.read_parquet(save_path)

# ipdb.set_trace()

# make pandas dataframe


df = pd.DataFrame(users)

import matplotlib.pyplot as plt

# plt.figure(figsize=(10, 6))
# plt.hist(df['tweets_count'], bins=1000, log=True)
# plt.xlabel('Number of Tweets')
# plt.ylabel('Frequency')
# plt.title('Distribution of Tweets Count')
# plt.show()



flat_users_df = pd.json_normalize(df['user'])

df['user_id'] = df['user'].apply(lambda x: x['id'])

df = df.drop_duplicates(subset=['user_id'])
flat_users_df = flat_users_df.drop_duplicates(subset=['id'])

try:
    merged_df = pd.merge(df, flat_users_df, left_on='user_id', right_on='id', how='left')
except:
    ipdb.set_trace()
# merged_df = pd.merge(df, flat_users_df, left_on='user', right_on='id', how='left')


# plt.figure(figsize=(10, 6))
# plt.hist(merged_df['likes_count'], bins=1000, log=True)
# plt.xlabel('Number of Likes')
# plt.ylabel('Frequency')
# plt.title('Distribution of Likes Count')
# plt.show()

# plt.figure(figsize=(10, 6))
# plt.hist(merged_df['friendsCount'], bins=1000, log=True)
# plt.xlabel('Number of Friends (Following)')
# plt.ylabel('Frequency')
# plt.title('Distribution of Friends Count (Following)')
# plt.show()

# def save_plots_to_file(dataframe, output_directory):
#     if not os.path.exists(output_directory):
#         os.makedirs(output_directory)

#     # Plot the distribution of tweets count
#     plt.figure(figsize=(10, 6))
#     plt.hist(dataframe['tweets_count'], bins=1000, log=True)
#     plt.xlabel('Number of Tweets')
#     plt.ylabel('Frequency')
#     plt.title('Distribution of Tweets Count')
#     plt.savefig(os.path.join(output_directory, 'tweets_count_distribution.png'))
#     plt.close()

#     # Plot the distribution of followers count
#     plt.figure(figsize=(10, 6))
#     plt.hist(dataframe['followersCount'], bins=1000, log=True)
#     plt.xlabel('Number of Followers')
#     plt.ylabel('Frequency')
#     plt.title('Distribution of Followers Count')
#     plt.savefig(os.path.join(output_directory, 'followers_count_distribution.png'))
#     plt.close()

#     # Plot the distribution of friends count (following)
#     plt.figure(figsize=(10, 6))
#     plt.hist(dataframe['friendsCount'], bins=1000, log=True)
#     plt.xlabel('Number of Friends (Following)')
#     plt.ylabel('Frequency')
#     plt.title('Distribution of Friends Count (Following)')
#     plt.savefig(os.path.join(output_directory, 'friends_count_distribution.png'))
#     plt.close()

#     # Plot the distribution of likes count
#     plt.figure(figsize=(10, 6))
#     plt.hist(dataframe['likes_count'], bins=1000, log=True)
#     plt.xlabel('Number of Likes')
#     plt.ylabel('Frequency')
#     plt.title('Distribution of Likes Count')
#     plt.savefig(os.path.join(output_directory, 'likes_count_distribution.png'))
#     plt.close()

#     # rank = favorite_count/tweet_count
#     dataframe['rank'] = dataframe['likes_count'] / dataframe['tweets_count']
#     # Plot the distribution of rank
#     plt.figure(figsize=(10, 6))
#     plt.hist(dataframe['rank'], bins=1000, log=True)
#     plt.xlabel('Rank')
#     plt.ylabel('Frequency')
#     plt.title('Distribution of Rank')
#     plt.savefig(os.path.join(output_directory, 'rank_distribution.png'))

# # Example usage

def save_plots_to_file(dataframe, output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Add scatter plots
    plot_variables = [
        ('tweets_count', 'followersCount', 'tweets_vs_followers.png'),
        ('tweets_count', 'friendsCount', 'tweets_vs_friends.png'),
        ('tweets_count', 'likes_count', 'tweets_vs_likes.png')
    ]

    for x_var, y_var, file_name in plot_variables:
        plt.figure(figsize=(10, 6))
        plt.scatter(dataframe[x_var], dataframe[y_var], alpha=0.3)
        plt.xlabel(x_var.replace('_', ' ').capitalize())
        plt.ylabel(y_var.replace('_', ' ').capitalize())
        plt.title(f'{x_var.replace("_", " ").capitalize()} vs {y_var.replace("_", " ").capitalize()}')
        plt.savefig(os.path.join(output_directory, file_name))
        plt.close()

    # Distribution plots
    plot_variables = [
        ('tweets_count', 'Distribution of Tweets Count', 'tweets_count_distribution.png'),
        ('followersCount', 'Distribution of Followers Count', 'followers_count_distribution.png'),
        ('friendsCount', 'Distribution of Friends Count (Following)', 'friends_count_distribution.png'),
        ('likes_count', 'Distribution of Likes Count', 'likes_count_distribution.png')
    ]

    for var, title, file_name in plot_variables:
        plt.figure(figsize=(10, 6))
        plt.hist(dataframe[var], bins=100, log=True)
        plt.xlabel(var.replace('_', ' ').capitalize())
        plt.ylabel('Frequency')
        plt.title(title)
        plt.savefig(os.path.join(output_directory, file_name))
        plt.close()

    # Rank distribution
    dataframe['tweetrank'] = dataframe['tweets_count'] / dataframe['likes_count']
    # infinite values are caused by 0 likes
    dataframe['tweetrank'] = dataframe['tweetrank'].replace([np.inf, -np.inf], -1)

    plt.figure(figsize=(10, 6))
    plt.hist(dataframe['tweetrank'], bins=1000, log=True)
    plt.xlabel('Tweet Rank')
    plt.ylabel('Frequency')
    plt.title('Distribution of Rank = Tweets Per Like')
    plt.savefig(os.path.join(output_directory, 'tweet_rank_distribution.png'))
    plt.close()
    # x  = index
    # y = value
    # x = dataframe['tweetrank'].value_counts().index
    # plt.figure(figsize=(10, 6))
    # plt.scatter(dataframe[x_var], dataframe[y_var], alpha=0.3)
    # plt.xlabel(x_var.replace('_', ' ').capitalize())
    # plt.ylabel(y_var.replace('_', ' ').capitalize())
    # plt.title(f'{x_var.replace("_", " ").capitalize()} vs {y_var.replace("_", " ").capitalize()}')
    # plt.savefig(os.path.join(output_directory, file_name))
    # plt.close()


output_directory = "./plots/pre/"

save_plots_to_file(merged_df, output_directory)

# plot tweets counts, followers, following, likes
ipdb.set_trace()



