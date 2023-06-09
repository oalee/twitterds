import os, yerbamate
import threading
import tqdm
import numpy as np
import pandas as pd
import igraph as ig
from collections import defaultdict
from itertools import combinations, chain

env = yerbamate.Environment()

path = os.path.join(env["plots"], "analysis", "user_hashtag.parquet")

df = pd.read_parquet(path)
tweet_dist_path = os.path.join(env["save"], "users", "tweets_distribution")

print("Filtering DataFrame...")

df = pd.read_parquet(path)

tweet_dist_df = pd.read_parquet(tweet_dist_path)

print("Filtering DataFrame...")

# filter out users that have less than 500 tweets (inactive users)
inactive_users = set(tweet_dist_df[tweet_dist_df["count"] < 500]["userId"])

# filter out inactive users
df = df[~df["userId"].isin(inactive_users)]
hashtag_counts = df.groupby("hashtag").size().reset_index(name="counts")
top_hashtags = hashtag_counts.sort_values("counts", ascending=False).head(1000)
# create a set of the top hashtags for faster lookup
top_hashtags_set = set(top_hashtags["hashtag"])

# filter the original DataFrame
df_filtered = df[df["hashtag"].isin(top_hashtags_set)]
df = df_filtered

# print number of unique users and hashtags
print(f"Number of unique users: {len(df['userId'].unique())}")
print(f"Number of unique hashtags: {len(df['hashtag'].unique())}")

print("Creating graph...")
# initialize a new graph
g = ig.Graph(directed=False)

# Add vertices
g.add_vertices(n=len(df["userId"].unique()))

# map users to unique integers
user_mapping = {user: i for i, user in enumerate(df["userId"].unique())}

# map back from index to user for later use
index_to_user = {i: user for user, i in user_mapping.items()}

# add the mapped user ids to the dataframe
df["user_id"] = df["userId"].map(user_mapping)

# create a dictionary where keys are hashtags and values are lists of users that used this hashtag
# hashtag_to_users = df.groupby('hashtag')['user_id'].apply(list).to_dict()

print("Computing edges...")

# create a dictionary where keys are hashtags and values are lists of users that used this hashtag
hashtag_to_users = df.groupby("hashtag")["user_id"].apply(list).to_dict()

# print("Computing edges...")


# g.es["weight"] = 0# DataFrame to store the final edges and weights
edges_df = pd.DataFrame(columns=["source", "target", "hashtag"])

if os.path.exists(os.path.join(env["plots"], "analysis", "edges.parquet")):
    edges_df = pd.read_parquet(os.path.join(env["plots"], "analysis", "edges.parquet"))


allready_calculated_hashtags = set(edges_df["hashtag"].unique())


for i, (hashtag, users) in enumerate(tqdm.tqdm(hashtag_to_users.items())):
    if hashtag in allready_calculated_hashtags:
        continue
    if len(users) > 1:
        # Generate all pairs of users.
        # df = pd.DataFrame(columns=["source", "target", "hashtag"])

        pairs_list = [
            (min(pair[0], pair[1]), max(pair[0], pair[1]), hashtag)
            for pair in combinations(users, 2)
        ]

        df = pd.DataFrame(pairs_list, columns=["source", "target", "hashtag"])

        edges_df = pd.concat([edges_df, df])  
        # on a thread, save the edges_df to a csv file
        # every 10 iterations
        if i % 10 == 0:
            edges_df.to_parquet(os.path.join(env["plots"], "analysis", "edges.parquet"))

edges_df.to_parquet(os.path.join(env["plots"], "analysis", "edges.parquet"))
