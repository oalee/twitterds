from transformers import  LlamaModel, LlamaTokenizer
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



class CustomDenoisingAutoEncoderLoss(losses.DenoisingAutoEncoderLoss):
    def __init__(self, model, custom_tokenizer, tie_encoder_decoder=True, *args, **kwargs):
        super().__init__(model, tie_encoder_decoder=False, *args, **kwargs)  # Set tie_encoder_decoder=False
        self.tokenizer_decoder = custom_tokenizer
        if tie_encoder_decoder:
            self.tokenizer_decoder = self.tokenizer_encoder
            self.decoder.resize_token_embeddings(len(self.tokenizer_decoder))



# Define your sentence transformer model using CLS pooling
model_name = 'bert-base-multilingual-uncased'

tokenizer = 
word_embedding_model = models.Transformer(model_name)


tokenizer_path = os.path.join(env['tokenizer_path'], 'tokenizer.model')

# ipdb.set_trace()
tokenizer = LlamaTokenizer(tokenizer_path)


word_embedding_model.tokenizer = tokenizer
word_embedding_model.vocab_size = tokenizer.vocab_size

pooling_model=models.Pooling(
    word_embedding_model.get_word_embedding_dimension(), 'cls')
model=SentenceTransformer(modules=[word_embedding_model, pooling_model])
# model = model.half()

train_dataloader=get_train_ds_sentences(
    size=10000, batch_size=4, shuffle=True)

print("Train dataloader loaded")

# get_train_dataloader(size=100000,batch_size=4, shuffle=True)

chkpt_save_steps=500
# load model if checkpoint exists

if env.restart:
    if os.path.exists(os.path.join(env["weights"], 'tsdae-model')):

        #   check for latest checkpoint

        start=chkpt_save_steps
        while True:
            if os.path.exists(os.path.join(env["weights"], 'tsdae-model', f'{start}')):
                start += chkpt_save_steps
            else:
                start -= chkpt_save_steps
                break

        last_chkpt=os.path.join(env["weights"], 'tsdae-model', f'{start}')
        if os.path.exists(last_chkpt):
            model=SentenceTransformer(last_chkpt, device='cuda')
            # #   set start to next checkpoint
            # start += chkpt_save_steps
            print(f'Loaded model from {last_chkpt}')


# Use the denoising auto-encoder loss
train_loss=losses.DenoisingAutoEncoderLoss(
    model, decoder_name_or_path=model_name, tie_encoder_decoder=True)




# ipdb.set_trace()
# Call the fit method
# if env.train:

model.fit(
    train_objectives=[(train_dataloader, train_loss)],
    epochs=1,
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
