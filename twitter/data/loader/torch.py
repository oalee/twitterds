from sentence_transformers import models, util, datasets, evaluation, losses
from torch.utils.data import DataLoader
from .prepro import get_train_sentences



def get_train_dataloader(shuffle=True, batch_size=16):
    train_sentences = get_train_sentences()

    train_dataset = datasets.DenoisingAutoEncoderDataset(train_sentences)
    train_dataloader = DataLoader(
        train_dataset, shuffle=shuffle, batch_size=batch_size)
    return train_dataloader
