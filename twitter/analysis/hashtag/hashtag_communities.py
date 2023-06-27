
import pandas as pd, os, yerbamate, ipdb
import igraph as ig
import yerbamate

env = yerbamate.Environment()

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

import igraph as ig

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


# save graph to file

save_path = os.path.join(env["plots"], "analysis", "users_hashtag_graph.graphml")
g.write(save_path, format="graphml")


path = os.path.join(env["plots"], "analysis", "user_hashtag_before.parquet")

df = pd.read_parquet(path)
 # Create a set of unique user IDs from the edge list dataframe
users = set(df_final['node1']).union(set(df_final['node2']))

# Create a mapping from user IDs to igraph node indices and vice versa
user_to_index = {user: index for index, user in enumerate(users)}
index_to_user = {index: user for index, user in enumerate(users)}

# Add the partition to the user dataframe
df['partition'] = [partition.membership[user_to_index[user]] for user in df['userId']]
