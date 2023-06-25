import os
import tqdm
import numpy as np
import pandas as pd
import igraph as ig
import sqlite3
from itertools import combinations

import yerbamate

env = yerbamate.Environment()

path = os.path.join(env["plots"], "analysis", "user_hashtag.parquet")

df = pd.read_parquet(path)

print("Filtering DataFrame...")

hashtag_counts = df.groupby("hashtag").size().reset_index(name="counts")
top_hashtags = hashtag_counts.sort_values("counts", ascending=False).head(1000)
top_hashtags_set = set(top_hashtags["hashtag"])

df = df[df["hashtag"].isin(top_hashtags_set)]

print(f"Number of unique users: {len(df['userId'].unique())}")
print(f"Number of unique hashtags: {len(df['hashtag'].unique())}")

print("Creating graph...")
g = ig.Graph(directed=False)
g.add_vertices(n=len(df["userId"].unique()))

user_mapping = {user: i for i, user in enumerate(df["userId"].unique())}
df["user_id"] = df["userId"].map(user_mapping)

print("Computing edges...")

hashtag_to_users = df.groupby("hashtag")["user_id"].apply(list).to_dict()

conn = sqlite3.connect("edges.db")
cursor = conn.cursor()
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS edges (
        source INTEGER,
        target INTEGER,
        hashtag TEXT,
        UNIQUE(source, target, hashtag)
    )
"""
)
conn.commit()

allready_calculated_hashtags = cursor.execute(
    """
    SELECT DISTINCT hashtag
    FROM edges
"""
).fetchall()

allready_calculated_hashtags = set([x[0] for x in allready_calculated_hashtags])

for i, (hashtag, users) in enumerate(tqdm.tqdm(hashtag_to_users.items())):
    if hashtag in allready_calculated_hashtags:
        continue
    if len(users) > 1:
        print("Computing edges for hashtag: ", hashtag)
        # pairs_list = [sorted(pair) for pair in combinations(users, 2)]
        
        # pairs = pd.DataFrame(pairs_list, columns=["source", "target"])
        # pairs["hashtag"] = hashtag
        # print("Inserting edges into database...")
        # pairs.to_sql("edges", conn, if_exists="append", index=False)
        # pairs = None  # Free memory
        # Create a placeholder for the SQL query
        query = 'INSERT INTO edges (source, target, hashtag) VALUES (?, ?, ?)'

        # Convert the pairs to a list of tuples
        pairs_list = [(min(pair[0], pair[1]), max(pair[0], pair[1]), hashtag) for pair in combinations(users, 2)]

        print("Inserting edges into database...")
        # Execute the query with executemany
        cursor.executemany(query, pairs_list)

        # Commit the changes to the database
        conn.commit()


edges_df = pd.read_sql_query(
    """
    SELECT source, target, COUNT(hashtag) as weight, GROUP_CONCAT(hashtag) as hashtags
    FROM edges
    GROUP BY source, target
""",
    conn,
)

g.add_edges(edges_df[["source", "target"]].values)
g.es["weight"] = edges_df["weight"].values
g.es["hashtags"] = edges_df["hashtags"].apply(lambda x: x.split(","))

print("Created Edgess, adding to graph...")

save_path = os.path.join(env["plots"], "analysis", "users_hashtag_graph.graphml")
g.write(save_path, format="graphml")

print("Number of vertices: ", len(g.vs))
print("Number of edges: ", len(g.es))

print("Finding communities...")
import leidenalg as la

partition = la.find_partition(g, la.ModularityVertexPartition)

print("Number of communities: ", len(partition))

import pickle


save_path = os.path.join(env["plots"], "analysis", "user_hashtag_partition.pickle")
with open(save_path, "wb") as f:
    pickle.dump(partition, f)
