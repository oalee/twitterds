import os
import ipdb
from pathlib import Path
import yerbamate
from transformers import AutoTokenizer

model_name = "bert-base-multilingual-uncased"

tokenizer = AutoTokenizer.from_pretrained(model_name)

# file list, each file is a tweet

env = yerbamate.Environment()


users_path = env['export']


all_tweets = [str(path) for path in Path(users_path).glob('*.txt')]


def file_lines_generator(all_tweets):
    for file_path in all_tweets:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield line.strip()


line_iterator = file_lines_generator(all_tweets)

#  update the tokenizer on all the tweets.txt files

tokenizer.train_new_from_iterator(
    line_iterator  # needs an interator of strings, but all_tweets is a list of file paths,
    , vocab_size=tokenizer.vocab_size
)


# save the tokenizer

os.makedirs('tokenizer-bert-updated', exist_ok=True)
tokenizer.save_pretrained('tokenizer-bert-updated')
