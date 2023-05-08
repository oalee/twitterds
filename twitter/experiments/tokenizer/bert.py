import os
from tokenizers import BertWordPieceTokenizer, SentencePieceBPETokenizer

from sentence_transformers import SentenceTransformer, LoggingHandler
from sentence_transformers import models, util, datasets, evaluation, losses
import yerbamate
from pathlib import Path
import ipdb
# file list, each file is a tweet

env = yerbamate.Environment()


users_path = env['export']


# find all the tweets.txt files

# all_tweets = Path(users_path).glob('*.txt')
# all_tweets = list(Path(users_path).glob('*.txt'))
all_tweets = [str(path) for path in Path(users_path).glob('*.txt')]

model_name = 'bert-base-multilingual-uncased'
word_embedding_model = models.Transformer(model_name)

# ipdb.set_trace()

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
