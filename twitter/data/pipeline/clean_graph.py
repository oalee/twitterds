import os, yerbamate

env = yerbamate.Environment()


import pandas as pd, os, yerbamate, ipdb
import igraph as ig
import yerbamate

env = yerbamate.Environment()


path = os.path.join(env["plots"], "analysis", "user_hashtag.parquet")

df = pd.read_parquet(path)

user_mapping = {user: i for i, user in enumerate(df["userId"].unique())}

index_to_user = {i: user for user, i in user_mapping.items()}




edge_path = os.path.join(env["plots"], "analysis", "edges.parquet")

df = pd.read_parquet(edge_path)

print(df.shape)


# data fram edges hash hashtag src tgt, we need to unordered it to small to big, then grouby src tgt to get weight
df = df.sort_values(by=["hashtag", "source", "target"], ascending=True)


import numpy as np

# Create two new columns 'node1' and 'node2', which are 'source' and 'target' sorted
df[['node1', 'node2']] = pd.DataFrame(np.sort(df[['source', 'target']], axis=1))

# Group by 'node1' and 'node2' and sum the weights for each pair
df_final = df.groupby(['node1', 'node2'])['weight'].sum().reset_index()
