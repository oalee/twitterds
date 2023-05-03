from transformers import LlamaTokenizer
import sentencepiece as spm

# Train a new tokenizer model with the combined data
from ...data.loader.torch import get_train_ds_sentences
import os

import yerbamate
import ipdb

env = yerbamate.Environment()


lamma_tokenizer_path = os.path.join(env["llama_tokenizer"], "tokenizer.model")

# train_dataloader, sentences_iterator = get_train_ds_sentences(
#     size=1000000, batch_size=4, shuffle=True)

old_tokenizer = LlamaTokenizer.from_pretrained(lamma_tokenizer_path)

# Get special tokens from the old tokenizer
special_tokens = list(old_tokenizer.special_tokens_map.values())

vocab_size = 12000

bos_id = old_tokenizer.convert_tokens_to_ids(
    old_tokenizer.special_tokens_map["bos_token"])
eos_id = old_tokenizer.convert_tokens_to_ids(
    old_tokenizer.special_tokens_map["eos_token"])
unk_id = old_tokenizer.convert_tokens_to_ids(
    old_tokenizer.special_tokens_map["unk_token"])


spm.SentencePieceTrainer.train(input=env["tokenizer_input_train"], model_prefix='updated_tokenizer',
                               vocab_size=vocab_size, model_type='bpe', bos_id=bos_id, eos_id=eos_id, unk_id=unk_id, bos_piece=old_tokenizer.special_tokens_map["bos_token"], eos_piece=old_tokenizer.special_tokens_map["eos_token"], unk_piece=old_tokenizer.special_tokens_map["unk_token"])
