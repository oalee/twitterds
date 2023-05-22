import torch
from ...data.loader.torch import get_train_dataloader, get_train_ds_sentences

from sentence_transformers import SentenceTransformer, LoggingHandler
from sentence_transformers import models, util, datasets, evaluation, losses
from transformers import AutoTokenizer, AutoModel

import yerbamate
import ipdb
import os
import logging
import tqdm
import numpy as np

from ...trainers.logger.tqdmlogger import TqdmLoggingHandler
from torch.utils.data import DataLoader


env = yerbamate.Environment()

env.print_info()

# Define your sentence transformer model using CLS pooling
model_name = 'bert-base-multilingual-uncased'
word_embedding_model = models.Transformer(model_name)

tokenizer = AutoTokenizer.from_pretrained('./tokenizer-bert-updated')
word_embedding_model.tokenizer = tokenizer

# user a fast tokenizer to update the vocab based on the new dat
pooling_model = models.Pooling(
    word_embedding_model.get_word_embedding_dimension(), 'cls')
model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
# model = model.half()

# size = 100_000_000
batch_size = env["batch_size"]

# if batch_size == None:

# train_dataloader, _ = get_train_ds_sentences(
#     size=size, batch_size=batch_size, shuffle=True)

root_data = os.path.join(env["data"], 'tweets')

train_files = os.listdir(root_data)
# join with root_data
train_files = [os.path.join(root_data, file) for file in train_files]

# print(f'\n\nFound {len(train_files)} files.\n\n')

print(f'\n\nLoading dataset.\n\n')


def noise_fn(text, del_ratio=0.6):
    words = tokenizer.tokenize(text)
    n = len(words)
    if n == 0:
        return text

    keep_or_not = np.random.rand(n) > del_ratio
    if sum(keep_or_not) == 0:
        keep_or_not[np.random.choice(n)] = True
    words = np.array(words)[keep_or_not]
    words_processed = tokenizer.convert_tokens_to_string(words)
    return words_processed


train_dataset = datasets.ListedDenoisingAutoEncoderDataset(
    train_files, noise_fn=noise_fn)

# print(f'\n\nDataset loaded.\n\n')

# ipdb.set_trace()

print(f'\n\nTraining on {len(train_dataset)} tweets.\n\n')


for i in tqdm.tqdm(range(len(train_dataset))):
    try:
        ot = train_dataset[i]
    except:
        print(f'Error at {i}')
        ipdb.set_trace()


train_dataloader = DataLoader(
    train_dataset, shuffle=False, batch_size=batch_size)

# test the data loader by iterating over it, check for errors

chkpt_save_steps = 500
# load model if checkpoint exists

if env.restart:
    if os.path.exists(os.path.join(env["weights"], 'tsdae-model')):

        #   check for latest checkpoint

        start = chkpt_save_steps
        while True:
            if os.path.exists(os.path.join(env["weights"], 'tsdae-model', f'{start}')):
                start += chkpt_save_steps
            else:
                start -= chkpt_save_steps
                break

        last_chkpt = os.path.join(env["weights"], 'tsdae-model', f'{start}')
        if os.path.exists(last_chkpt):
            model = SentenceTransformer(last_chkpt, device='cuda')
            # #   set start to next checkpoint
            # start += chkpt_save_steps
            print(f'Loaded model from {last_chkpt}')


# Use the denoising auto-encoder loss
train_loss = losses.DenoisingAutoEncoderLoss(
    model, decoder_name_or_path=model_name, tie_encoder_decoder=True)


# ipdb.set_trace()
# Call the fit method
# if env.train:

model.fit(
    train_objectives=[(train_dataloader, train_loss)],
    epochs=1,
    weight_decay=0.01,
    scheduler='WarmupLinear',
    optimizer_params={'lr': 3e-4},
    show_progress_bar=True,
    checkpoint_path=os.path.join(env["weights"], 'tsdae-model'),
    checkpoint_save_steps=chkpt_save_steps,
    output_path=os.path.join(env["weights"], 'tsdae-model', 'output'),
    
    # callback=LoggingHandler()
    # logger=logger

)

try:

    model.save(os.path.join(env["weights"], 'tsdae-model'))
except:
    ipdb.set_trace()
