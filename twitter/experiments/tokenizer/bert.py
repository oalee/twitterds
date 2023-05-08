import os
from tokenizers import BertWordPieceTokenizer, SentencePieceBPETokenizer

import yerbamate
from pathlib import Path
# file list, each file is a tweet

env = yerbamate.Environment()


users_path = env['users']


# find all the tweets.txt files

all_tweets = Path(users_path).glob('**/tweets.txt')

# create a tokenizer
tokenizer = SentencePieceBPETokenizer()


# train the tokenizer on all the tweets.txt files
tokenizer.train(all_tweets,
                vocab_size=32_000,
                min_frequency=2,
                special_tokens=['[PAD]', '[UNK]', '[CLS]', '[SEP]', '[MASK]'],
                show_progress=True,
                limit_alphabet=1000,
                )

# save the tokenizer
os.makedirs('tokenizer-32k', exist_ok=True)
tokenizer.save_model('tokenizer-32k')
