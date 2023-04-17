from ...data.loader.torch import get_train_dataloader

from sentence_transformers import SentenceTransformer, LoggingHandler
from sentence_transformers import models, util, datasets, evaluation, losses

import yerbamate, ipdb, os, logging, tqdm

from ...trainers.logger.tqdmlogger import TqdmLoggingHandler

env = yerbamate.Environment()

# Define your sentence transformer model using CLS pooling
model_name = 'bert-base-multilingual-uncased'
word_embedding_model = models.Transformer(model_name)
pooling_model = models.Pooling(
    word_embedding_model.get_word_embedding_dimension(), 'cls')
model = SentenceTransformer(modules=[word_embedding_model, pooling_model])

train_dataloader = get_train_dataloader(size=100000,batch_size=4, shuffle=True)

# Use the denoising auto-encoder loss
train_loss = losses.DenoisingAutoEncoderLoss(
    model, decoder_name_or_path=model_name, tie_encoder_decoder=True)

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# logger.addHandler(logging.StreamHandler(tqdm.tqdm.write))

# ipdb.set_trace()
# Call the fit method
if env.train:
    
    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        epochs=1,
        weight_decay=0.01,
        scheduler='constantlr',
        optimizer_params={'lr': 3e-4},
        show_progress_bar=True,
        checkpoint_path=os.path.join(env["weights"], 'tsdae-model'),
        # callback=LoggingHandler()
        # logger=logger
    
    )

    try:

        model.save(os.path.join(env["weights"], 'tsdae-model'))
    except:
        ipdb.set_trace()
