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

print("Filtering DataFrame...")

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
edges_df = pd.DataFrame(columns=["source", "target", "weight"])

if os.path.exists(os.path.join(env["plots"], "analysis", "edges.csv")):
    edges_df = pd.read_csv(os.path.join(env["plots"], "analysis", "edges.csv"))

# saved_edge_df = pd.read_csv(os.path.join(env["plots"], "analysis", "edges.csv")) if os.path.exists( os.path.join(env["plots"], "analysis", "edges.csv")) else 


def save_edges(edges_df):
    
    # append to existing file with fastparquet
    import fastparquet
    fastparquet.write(os.path.join(env["plots"], "analysis", "edges.parquet"), edges_df, append=True)
    # edges_df.to_csv(os.path.join(env["plots"], "analysis", "edges.csv"), index=False, mode="a", header=False)


# Iterate over the user lists in the dictionary.
for i, users in enumerate(tqdm.tqdm(hashtag_to_users.values())):
    if len(users) > 1:
        # Generate all pairs of users.
        pairs = pd.DataFrame(list(combinations(users, 2)), columns=["source", "target"])
        pairs["hashtag"] =  hashtag_to_users.keys()[i]
        # Add to the overall DataFrame and sum up the weights.
        edges_df = pd.concat([edges_df, pairs])

        # on a thread, save the edges_df to a csv file
        # every 10 iterations
        if i % 10 == 0:
            save_edges(edges_df)
            # clean cache by deleting the edges_df
            edges_df = pd.DataFrame(columns=["source", "target", "weight"])




# Ensure the source is always the smaller id and target is the larger id
edges_df[["source", "target"]] = np.sort(edges_df[["source", "target"]].values, axis=1)

# Drop duplicate edges

edges_df = edges_df.drop_duplicates()

# Now sum up the weights for each unique pair
edges_df = edges_df.groupby(["source", "target"]).sum().reset_index()

# Add the edges to the graph
g.add_edges(edges_df[["source", "target"]].values)

# Set the edge weights
g.es["weight"] = edges_df["weight"].values


print("Created Edgess, adding to graph...")

# g.add_edges(edges)


# save graph to file
# save_path = os.path.join(env['plots'], 'analysis', 'user_hashtag_graph.gml')
save_path = os.path.join(env["plots"], "analysis", "users_hashtag_graph.graphml")
g.write(save_path, format="graphml")

print("Number of vertices: ", len(g.vs))
print("Number of edges: ", len(g.es))

print("Finding communities...")
# import the leidenalg library
import leidenalg as la

# get the partition using the Leiden algorithm
partition = la.find_partition(g, la.ModularityVertexPartition)

print("Number of communities: ", len(partition))

# save the partition to a file with pickle
import pickle

save_path = os.path.join(env["plots"], "analysis", "user_hashtag_partition.pickle")
with open(save_path, "wb") as f:
    pickle.dump(partition, f)
