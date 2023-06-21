import re


def clean_tweet(text):
    # Remove mentions
    text = re.sub(r"@\w+", "", text)

    # Remove URLs
    text = re.sub(r"http\S+|https\S+", "", text)

    # Remove newlines
    text = text.replace("\n", " ")

    # Remove extra whitespaces
    text = re.sub(r"\s+", " ", text).strip()

    # Remove "RT :"
    text = re.sub(r"RT :", "", text)

    return text
