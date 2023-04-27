
import seaborn as sns
import matplotlib.pyplot as plt
import os
import numpy as np
from ..loader.prepro import get_users



import os
import numpy as np
import matplotlib.pyplot as plt

def     display_plots(dataframe):
    # Scatter plots
    plot_variables = [
        ('tweets_count', 'followers_count'),
        ('tweets_count', 'friends_count'),
        ('tweets_count', 'likes_count')
    ]

    for x_var, y_var in plot_variables:
        plt.figure(figsize=(10, 6))
        plt.scatter(dataframe[x_var], dataframe[y_var], alpha=0.3)
        plt.xlabel(x_var.replace('_', ' ').capitalize())
        plt.ylabel(y_var.replace('_', ' ').capitalize())
        plt.title(f'{x_var.replace("_", " ").capitalize()} vs {y_var.replace("_", " ").capitalize()}')
        plt.show()

    # Distribution plots
    plot_variables = [
        ('tweets_count', 'Distribution of Tweets Count'),
        ('followers_count', 'Distribution of Followers Count'),
        ('friends_count', 'Distribution of Friends Count'),
        ('likes_count', 'Distribution of Likes Count')
    ]

    for var, title in plot_variables:
        plt.figure(figsize=(10, 6))
        plt.hist(dataframe[var], bins=70, log=True)
        plt.xlabel(var.replace('_', ' ').capitalize())
        plt.ylabel('Frequency')
        plt.title(title)
        plt.show()

    # Rank distribution
    dataframe['tweetrank'] = dataframe['likes_count'] / dataframe['tweets_count']
    dataframe['tweetrank'] = dataframe['tweetrank'].replace([np.inf, -np.inf], -10)

    plt.figure(figsize=(10, 6))
    plt.hist(dataframe['tweetrank'], bins=1000, log=True)
    plt.xlabel('Tweet Rank')
    plt.ylabel('Frequency')
    plt.title('Distribution of Rank Index=Like/Tweet')
    plt.show()

    # Define rtweetrank
    dataframe['rtweetrank'] = dataframe['retweets_count'] / dataframe['tweets_count']
    dataframe['rtweetrank'] = dataframe['rtweetrank'].replace([np.inf, -np.inf], -10)
    
    plt.figure(figsize=(10, 6))
    plt.hist(dataframe['rtweetrank'], bins=1000, log=True)
    plt.xlabel('Retweet Rank')
    plt.ylabel('Frequency')
    plt.title('Distribution of ReTweetRank Index=Retweet/Tweet')
    plt.show()


def save_plots_to_file(dataframe, output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Add scatter plots
    plot_variables = [
        ('tweets_count', 'followers_count', 'tweets_vs_followers.png'),
        ('tweets_count', 'friends_count', 'tweets_vs_friends.png'),
        ('tweets_count', 'likes_count', 'tweets_vs_likes.png')
    ]

    for x_var, y_var, file_name in plot_variables:
        plt.figure(figsize=(10, 6))
        plt.scatter(dataframe[x_var], dataframe[y_var], alpha=0.3)
        plt.xlabel(x_var.replace('_', ' ').capitalize())
        plt.ylabel(y_var.replace('_', ' ').capitalize())
        plt.title(
            f'{x_var.replace("_", " ").capitalize()} vs {y_var.replace("_", " ").capitalize()}')
        plt.savefig(os.path.join(output_directory, file_name))
        plt.close()

    # Distribution plots
    plot_variables = [
        ('tweets_count', 'Distribution of Tweets Count',
         'tweets_count_distribution.png'),
        ('followers_count', 'Distribution of Followers Count',
         'followers_count_distribution.png'),
        ('friends_count', 'Distribution of Friends Count',
         'friends_count_distribution.png'),
        ('likes_count', 'Distribution of Likes Count', 'likes_count_distribution.png')
    ]

    for var, title, file_name in plot_variables:
        plt.figure(figsize=(10, 6))
        plt.hist(dataframe[var], bins=70, log=True)
        plt.xlabel(var.replace('_', ' ').capitalize())
        plt.ylabel('Frequency')
        plt.title(title)
        plt.savefig(os.path.join(output_directory, file_name))
        plt.close()

    # Rank distribution
    dataframe['tweetrank'] = dataframe['likes_count'] /   dataframe['tweets_count']
    # infinite values are caused by 0 likes
    dataframe['tweetrank'] = dataframe['tweetrank'].replace(
        [np.inf, -np.inf], -10)

    plt.figure(figsize=(10, 6))
    plt.hist(dataframe['tweetrank'], bins=1000, log=True)
    plt.xlabel('Tweet Rank')
    plt.ylabel('Frequency')
    plt.title('Distribution of Rank Index=Like/Tweet ')
    plt.savefig(os.path.join(output_directory, 'tweet_rank_distribution.png'))
    plt.close()



    # define rtweetrank
    dataframe['rtweetrank'] = dataframe['retweets_count'] /   dataframe['tweets_count']
    # infinite values are caused by 0 likes
    dataframe['rtweetrank'] = dataframe['rtweetrank'].replace(
        [np.inf, -np.inf], -10)
    
    plt.figure(figsize=(10, 6))
    plt.hist(dataframe['rtweetrank'], bins=1000, log=True)
    plt.xlabel('Retweet Rank')
    plt.ylabel('Frequency')
    plt.title('Distribution of ReTweetRank Index=Retweet/Tweet ')


def plot_users(dataframe):
    plot_variables = [
        ('tweets_count', 'followers_count'),
        ('tweets_count', 'friends_count'),
        ('tweets_count', 'likes_count')
    ]

    fig, axs = plt.subplots(len(plot_variables), 1,
                            figsize=(10, 6 * len(plot_variables)))

    for idx, (x_var, y_var) in enumerate(plot_variables):
        axs[idx].scatter(dataframe[x_var], dataframe[y_var], alpha=0.3)
        axs[idx].set_xlabel(x_var.replace('_', ' ').capitalize())
        axs[idx].set_ylabel(y_var.replace('_', ' ').capitalize())
        axs[idx].set_title(
            f'{x_var.replace("_", " ").capitalize()} vs {y_var.replace("_", " ").capitalize()}')

    plt.tight_layout()
    plt.show()


def plot_columns(dataframe, columns):
    if len(columns) != 2:
        raise ValueError("Please provide exactly 2 columns for plotting.")

    x_var, y_var = columns

    plt.figure(figsize=(10, 6))
    plt.scatter(dataframe[x_var], dataframe[y_var], alpha=0.3)
    plt.xlabel(x_var.replace('_', ' ').capitalize())
    plt.ylabel(y_var.replace('_', ' ').capitalize())
    plt.title(
        f'{x_var.replace("_", " ").capitalize()} vs {y_var.replace("_", " ").capitalize()}')
    plt.show()


def plot_distribution(dataframe, column):
    plt.figure(figsize=(10, 6))
    sns.histplot(dataframe[column], kde=True, bins=100)
    plt.xlabel(column.replace('_', ' ').capitalize())
    plt.ylabel('Frequency')
    plt.title(f'Distribution of {column.replace("_", " ").capitalize()}')
    plt.show()


def plot_boxplot(dataframe, column):
    plt.figure(figsize=(10, 6))
    sns.boxplot(x=dataframe[column])
    plt.xlabel(column.replace('_', ' ').capitalize())
    plt.title(f'Boxplot of {column.replace("_", " ").capitalize()}')
    plt.show()


users = get_users()

# parse date such as month_day_timestamp from now
# file_name =
out_dir = "plots/pre/"

save_plots_to_file(users, out_dir)
