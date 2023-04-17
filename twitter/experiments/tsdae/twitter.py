from ...data.loader.torch import get_train_dataloader

from sentence_transformers import SentenceTransformer, LoggingHandler
from sentence_transformers import models, util, datasets, evaluation, losses

import yerbamate
import os

# Define your sentence transformer model using CLS pooling
model_name = 'sentence-transformers/distiluse-base-multilingual-cased-v2'
word_embedding_model = models.Transformer(model_name)
pooling_model = models.Pooling(
    word_embedding_model.get_word_embedding_dimension(), 'cls')
model = SentenceTransformer(modules=[word_embedding_model, pooling_model])

# Define a list with sentences (1k - 100k sentences)
# train_sentences = ["Your set of sentences",
#                    "Model will automatically add the noise",
#                    "And re-construct it",
#                    "You should provide at least 1k sentences"]

# Create the special denoising dataset that adds noise on-the-fly
# train_dataset = datasets.DenoisingAutoEncoderDataset(train_sentences)

# DataLoader to batch your data
# DataLoader(train_dataset, batch_size=8, shuffle=True)
train_dataloader = get_train_dataloader()

# Use the denoising auto-encoder loss
train_loss = losses.DenoisingAutoEncoderLoss(
    model, decoder_name_or_path=model_name, tie_encoder_decoder=True)


env = yerbamate.Environment()


# Call the fit method

if env.train:
    
    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        epochs=1,
        weight_decay=0,
        scheduler='constantlr',
        optimizer_params={'lr': 3e-5},
        show_progress_bar=True
    )

    model.save(os.path.join(env["results"], 'output/tsdae-model'))
