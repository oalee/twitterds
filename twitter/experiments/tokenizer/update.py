from torch.utils.data import DataLoader
from transformers import AutoTokenizer, AutoModel, LlamaTokenizer
from collections import defaultdict
from ...data.loader.torch import get_train_dataloader, get_train_ds_sentences

from sentence_transformers import SentenceTransformer, LoggingHandler
from sentence_transformers import models, util, datasets, evaluation, losses
from tokenizers import Tokenizer, models, pre_tokenizers, decoders, trainers, processors

import yerbamate
import ipdb
import os
import logging
import tqdm

from ...trainers.logger.tqdmlogger import TqdmLoggingHandler

env = yerbamate.Environment()


# Define your sentence transformer model using CLS pooling
# model_name = os.path.join(env["weights"], 'tsdae-model', f'500')
# word_embedding_model = models.Transformer(model_name)

lamma_tokenizer_path = os.path.join(env["weights"], "tokenizer.model")

train_dataloader, sen = get_train_ds_sentences(
    size=1000000, batch_size=4, shuffle=True)

old_tokenizer = LlamaTokenizer.from_pretrained(lamma_tokenizer_path)

special_tokens = list(old_tokenizer.special_tokens_map.values())


print(old_tokenizer.vocab_size)

trainer = trainers.BpeTrainer(old_tokenizer.vocab_size, special_tokens=special_tokens)
# tokenizer = old_tokenizer.train_new_from_iterator(sen, 2000)



ipdb.set_trace()