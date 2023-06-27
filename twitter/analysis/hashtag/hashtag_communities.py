
import pandas as pd, os, yerbamate, ipdb
import igraph as ig
import yerbamate
import vaex  

env = yerbamate.Environment()

period = "before"

edge_path = os.path.join(env["plots"], "analysis", f"edges_{period}_cleaned.parquet")

df = pd.read_parquet(edge_path)

print(df.shape)


df.rename(columns={"hashtag": "weight"}, inplace=True)
df = df[df["weight"] > 3]

print(df.shape)

# map node ids to start from 0
# node mapping is for both node_1 and node_2

all_nodes = set(df['source'].unique()).union(set(df['target'].unique()))

node_mapping = {node: i for i, node in enumerate(all_nodes)}
node_reverse_mapping = {i: node for i, node in enumerate(all_nodes)}

import igraph as ig

df_final = df.copy()

# we need to map the node ids to start from 0
df_final['source'] = df_final['source'].map(node_mapping)
df_final['target'] = df_final['target'].map(node_mapping)

# Create a graph from the dataframe
g = ig.Graph.TupleList(df_final.itertuples(index=False), directed=False, weights=True)



# number of edges and vertices
print("Number of vertices: ", len(g.vs))
print("Number of edges: ", len(g.es))

# get the partition using the Leiden algorithm
import leidenalg as la

partition = la.find_partition(g, la.ModularityVertexPartition, weights="weight")

print("Number of communities: ", len(partition))

# add the partition to the graph
g.vs["partition"] = partition.membership


path = os.path.join(env["plots"], "analysis", f"user_hashtag_{period}.parquet")

df_hashtags = pd.read_parquet(path)
 
    
# now add node id to user_hashtag dataframe
df_hashtags["node_id"] = df_hashtags["userId"].map(node_mapping).astype(int)

# add partition to user_hashtag dataframe

df_hashtags = df_hashtags.dropna(subset=["partition"])

