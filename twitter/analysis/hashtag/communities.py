import os, yerbamate
import tqdm
import pandas as pd
import igraph as ig
from collections import defaultdict
from itertools import combinations, chain

env = yerbamate.Environment()

path = os.path.join(env["plots"], "analysis", "user_hashtag.parquet")

df = pd.read_parquet(path)

print("Filtering DataFrame...")

hashtag_counts = df.groupby("hashtag").size().reset_index(name="counts")
top_hashtags = hashtag_counts.sort_values("counts", ascending=False).head(2000)
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


# g.es["weight"] = 0

# for users in hashtag_to_users.values():
#     if len(users) > 1:
#         for pair in combinations(users, 2):
#             # Create an edge identifier (smaller_id, larger_id)
#             edge_id = (min(pair[0], pair[1]), max(pair[0], pair[1]))
#             if g.are_connected(*edge_id):
#                 # If the edge exists, increase its weight by 1
#                 edge_index = g.get_eid(*edge_id)
#                 g.es[edge_index]["weight"] += 1
#             else:
#                 # If the edge does not exist, create it and set the weight to 1
#                 g.add_edge(*edge_id)
#                 edge_index = g.get_eid(*edge_id)
#                 g.es[edge_index]["weight"] = 1


# Create a dictionary to hold edge weights.
edge_weights = defaultdict(int)

# Iterate over the user lists in the dictionary.
for users in tqdm.tqdm(hashtag_to_users.values()):
    if len(users) > 1:
        # Iterate over each pair of users.
        for pair in combinations(users, 2):
            # Create an edge identifier (smaller_id, larger_id)
            edge_id = (min(pair), max(pair))
            # Increase the weight of this edge in our dictionary.
            edge_weights[edge_id] = 1

# Now that we've calculated all the edge weights, we can add the edges to the graph.
g.add_edges(edge_weights.keys())

# Finally, set the edge weights in the graph.
# g.es["weight"] = list(edge_weights.values())

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
