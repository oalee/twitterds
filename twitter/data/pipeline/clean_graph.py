 


import pandas as pd, os, yerbamate, ipdb
import igraph as ig
import yerbamate

env = yerbamate.Environment()

period = env.action if env.action != None else "before"



edges_path = os.path.join(env["plots"], "analysis", f"edges_{period}.parquet")

print("Loading DataFrame...",  edges_path)


df = pd.read_parquet(edges_path)


print("Cleaning DataFrame...")

# Sort DataFrame
df = df.sort_values(['source', 'target'])

print("Grouping DataFrame...")
# Group by 'source' and 'target', and then count unique 'hashtags'
df_grouped = df.groupby(['source', 'target']).agg({'hashtag': 'nunique'})


# Reset index to have 'source' and 'target' as columns again
df_grouped = df_grouped.reset_index()


print("Filtering DataFrame...")
# drop hashtag count 1 and duplicates

df_grouped = df_grouped[df_grouped['hashtag'] > 1]

df_grouped = df_grouped.drop_duplicates(['source', 'target'])

# save to parquet
save_path = os.path.join(env['plots'], 'analysis', f'edges_{period}_cleaned.parquet')


print("Saving DataFrame...", save_path)

df_grouped.to_parquet(save_path)


print("Saved! :)")