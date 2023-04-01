import snscrape.modules.twitter as twitter




def scrape_tweets(search_term, start_date, end_date, max_tweets):
    """Scrape tweets from Twitter using snscrape.
    Args:
        search_term (str): The search term to scrape tweets for.
        start_date (str): The start date to scrape tweets from.
        end_date (str): The end date to scrape tweets to.
        max_tweets (int): The maximum number of tweets to scrape.
    Returns:
        tweets (list): A list of tweets.
    """
    tweets = []
    for i, tweet in enumerate(twitter.TwitterSearchScraper(f'{search_term} since:{start_date} until:{end_date}').get_items()):
        if i > max_tweets:
            break
        tweets.append(tweet)
    return tweets

def scrape_hashtags(search_term, start_date, end_date, max_tweets):
    """Scrape hashtags from Twitter using snscrape.
    Args:
        search_term (str): The search term to scrape tweets for.
        start_date (str): The start date to scrape tweets from.
        end_date (str): The end date to scrape tweets to.
        max_tweets (int): The maximum number of tweets to scrape.
    Returns:
        hashtags (list): A list of hashtags.
    """
    hashtags = []
    for i, tweet in enumerate(twitter.TwitterSearchScraper(f'{search_term} since:{start_date} until:{end_date}').get_items()):
        if i > max_tweets:
            break
        hashtags.append(tweet.hashtags)
    return hashtags

def scrape_profile_info(search_term, start_date, end_date, max_tweets):
    """Scrape profile info from Twitter using snscrape.
    Args:
        search_term (str): The search term to scrape tweets for.
        start_date (str): The start date to scrape tweets from.
        end_date (str): The end date to scrape tweets to.
        max_tweets (int): The maximum number of tweets to scrape.
    Returns:
        profile_info (list): A list of profile info.
    """
    profile_info = []
    for i, tweet in enumerate(twitter.TwitterSearchScraper(f'{search_term} since:{start_date} until:{end_date}').get_items()):
        if i > max_tweets:
            break
        profile_info.append(tweet.user)
    return profile_info

def scrape_tweet_id(tweet_id):
    """Scrape a single tweet from Twitter using snscrape.
    Args:
        tweet_id (int): The tweet ID to scrape.
    Returns:
        tweet (list): A list of tweets.
    """
    tweet = []
    for i, tweet in enumerate(twitter.TwitterSearchScraper(f'id:{tweet_id}').get_items()):
        if i > 1:
            break
        tweet.append(tweet)
    return tweet