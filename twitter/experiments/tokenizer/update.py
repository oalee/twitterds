from ...data.loader.torch import get_train_dataloader, get_train_ds_sentences

from sentence_transformers import SentenceTransformer, LoggingHandler
from sentence_transformers import models, util, datasets, evaluation, losses

import yerbamate
import ipdb
import os
import logging
import tqdm

from ...trainers.logger.tqdmlogger import TqdmLoggingHandler

env = yerbamate.Environment()

from collections import defaultdict

from transformers import AutoTokenizer, AutoModel
from torch.utils.data import DataLoader
from sentence_transformers import models, util, datasets, evaluation, losses



# Define your sentence transformer model using CLS pooling
model_name = os.path.join(env["weights"], 'tsdae-model', f'500')
# word_embedding_model = models.Transformer(model_name)


train_dataloader, sen = get_train_ds_sentences(
    size=1000000, batch_size=4, shuffle=True)

old_tokenizer = AutoTokenizer.from_pretrained(model_name)


ipdb.set_trace()
print(old_tokenizer.vocab_size)
tokenizer = old_tokenizer.train_new_from_iterator(sen, 2000)
