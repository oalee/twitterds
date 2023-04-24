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

def update_tokenizer_and_model(model_name, sentences, batch_size=64):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    new_tokens = set()
    unk_token_id = tokenizer.convert_tokens_to_ids(tokenizer.unk_token)
    corpus = sentences

    word_freqs = defaultdict(int)
    for text in corpus:
        words_with_offsets = tokenizer.backend_tokenizer.pre_tokenizer.pre_tokenize_str(text)
        new_words = [word for word, offset in words_with_offsets]
        for word in new_words:
            word_freqs[word] += 1

    ipdb.set_trace()

    for i in tqdm.trange(0, len(sentences), batch_size):
        batch_sentences = sentences[i:i + batch_size]
        tokenized_batch = [tokenizer.tokenize(sent) for sent in batch_sentences]
        # unique_batch_tokens = set(token for sent_tokens in tokenized_batch for token in sent_tokens)
        
        
        for sent_tokens in tokenized_batch:
            for token in sent_tokens:
                token_id = tokenizer.convert_tokens_to_ids(token)
                if token_id == unk_token_id:
                    new_tokens.add(token)

    tokenizer_vocab_set = set(tokenizer.get_vocab().keys())
    unk_token_id = tokenizer.convert_tokens_to_ids(tokenizer.unk_token)

    n_tokens = [token for token in new_tokens if tokenizer.convert_tokens_to_ids(token) == unk_token_id]

    # new_tokens = [token for token in new_tokens if token not in tokenizer.get_vocab()]

    if n_tokens:
        ipdb.set_trace()
        num_added_tokens = tokenizer.add_tokens(new_tokens)
        model.resize_token_embeddings(len(tokenizer))
        print(f"Number of tokens added: {num_added_tokens}")

        tokenizer.save_pretrained("path/to/updated/tokenizer")
        model.save_pretrained("path/to/updated/model")

        return "path/to/updated/model"
    else:
        print("No new tokens found.")
        return model_name

# Define your sentence transformer model using CLS pooling
model_name = os.path.join(env["weights"], 'tsdae-model', f'500')
word_embedding_model = models.Transformer(model_name)


train_dataloader, sen = get_train_ds_sentences(
    size=100000, batch_size=4, shuffle=True)

update_tokenizer_and_model(model_name, sen)

