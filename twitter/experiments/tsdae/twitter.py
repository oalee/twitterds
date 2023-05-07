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

# Define your sentence transformer model using CLS pooling
model_name = 'bert-base-multilingual-uncased'
word_embedding_model = models.Transformer(model_name)


# user a fast tokenizer to update the vocab based on the new dat
pooling_model = models.Pooling(
    word_embedding_model.get_word_embedding_dimension(), 'cls')
model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
# model = model.half()

size = 100_000_000
batch_size = 128

train_dataloader, _ = get_train_ds_sentences(
    size=size, batch_size=batch_size, shuffle=True)

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
    epochs=10,
    weight_decay=0.01,
    scheduler='constantlr',
    optimizer_params={'lr': 3e-4},
    show_progress_bar=True,
    checkpoint_path=os.path.join(env["weights"], 'tsdae-model'),
    checkpoint_save_steps=chkpt_save_steps,
    # callback=LoggingHandler()
    # logger=logger

)

try:

    model.save(os.path.join(env["weights"], 'tsdae-model'))
except:
    ipdb.set_trace()
