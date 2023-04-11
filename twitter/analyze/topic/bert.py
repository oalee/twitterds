from sentence_transformers import SentenceTransformer
from ...data.loader.prepro import get_user, get_cleaned_tweets
import ipdb, umap, hdbscan
import pandas as pd

from bertopic import BERTopic

model = SentenceTransformer('paraphrase-MiniLM-L3-v2')


data = get_cleaned_tweets("Maggy18370661")

topic_model = BERTopic(embedding_model=model, calculate_probabilities=True, verbose=True)
topics, probabilities = topic_model.fit_transform(data)

# embeddings = model.encode(data, show_progress_bar=True)

# umap_embeddings = umap.UMAP(n_neighbors=15, 
#                             n_components=5, 
#                             metric='cosine').fit_transform(embeddings)

# cluster = hdbscan.HDBSCAN(min_cluster_size=15,
#                           metric='euclidean',                      
#                           cluster_selection_method='eom').fit(umap_embeddings)

# import matplotlib.pyplot as plt

# # Prepare data
# umap_data = umap.UMAP(n_neighbors=15, n_components=2, min_dist=0.0, metric='cosine').fit_transform(embeddings)
# result = pd.DataFrame(umap_data, columns=['x', 'y'])
# result['labels'] = cluster.labels_

# # Visualize clusters
# fig, ax = plt.subplots(figsize=(20, 10))
# outliers = result.loc[result.labels == -1, :]
# clustered = result.loc[result.labels != -1, :]
# plt.scatter(outliers.x, outliers.y, color='#BDBDBD', s=0.05)
# plt.scatter(clustered.x, clustered.y, c=clustered.labels, s=0.05, cmap='hsv_r')
# plt.colorbar()

ipdb.set_trace()