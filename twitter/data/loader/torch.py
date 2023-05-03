from sentence_transformers import models, util, datasets, evaluation, losses
from torch.utils.data import DataLoader
# from .prepro import get_train_sentences

import yerbamate
import os
import vaex

env = yerbamate.Environment()


def get_train_sentences(size):

    # return env["data"], "tweets.hdf5", open, to array, and return
    path = os.path.join(
        env["local_d"], "tweets.hdf5"
    )

    df = vaex.open(path)

    # select randomly size of df size
    sample_df = df.sample(n=size)
    return sample_df["text"].tolist()

    


def get_train_dataloader(size=100000, shuffle=True, batch_size=16):
    train_sentences = get_train_sentences(size)

    train_dataset = datasets.DenoisingAutoEncoderDataset(train_sentences)
    train_dataloader = DataLoader(
        train_dataset, shuffle=shuffle, batch_size=batch_size)
    return train_dataloader


def get_train_ds_sentences(size=100000, shuffle=True, batch_size=16):
    train_sentences = get_train_sentences(size)

    train_dataset = datasets.DenoisingAutoEncoderDataset(train_sentences)
    train_dataloader = DataLoader(
        train_dataset, shuffle=shuffle, batch_size=batch_size)
    return train_dataloader, train_sentences
