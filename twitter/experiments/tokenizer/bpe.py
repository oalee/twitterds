from tokenizers import Tokenizer, pre_tokenizers, decoders, trainers
from tokenizers.models import BPE
from transformers import LlamaTokenizer
import os

from ...data.loader.torch import get_train_ds_sentences

import yerbamate
import ipdb

env = yerbamate.Environment()

# Save the old tokenizer's vocab and merge files
lamma_tokenizer_path = os.path.join(env["weights"], "tokenizer.model")

train_dataloader, sentences_iterator = get_train_ds_sentences(
    size=1000000, batch_size=4, shuffle=True)

old_tokenizer = LlamaTokenizer.from_pretrained(lamma_tokenizer_path)

special_tokens = list(old_tokenizer.special_tokens_map.values())


vocab_file_path = os.path.join(env['weights'], 'vocab', 'vocab.txt')
merges_file_path = os.path.join(env['weights'], 'vocab', 'merges.txt')

# os mkdir
os.makedirs(
    os.path.join(env["weights"], "vocab"), exist_ok=True
)

old_tokenizer.save_vocabulary(save_directory=os.path.dirname(
    vocab_file_path), filename_prefix='vocab')

ipdb.set_trace()

# Create a new tokenizer using the old vocab and merge files
tokenizer = Tokenizer(BPE.from_file(vocab_file_path, merges_file_path))
tokenizer.pre_tokenizer = pre_tokenizers.ByteLevel()
tokenizer.decoder = decoders.ByteLevel()
tokenizer.post_processor = pre_tokenizers.post_processors.ByteLevelTemplateProcessing(
    single=old_tokenizer.special_tokens_map["bos_token"] +
    "${0}" + old_tokenizer.special_tokens_map["eos_token"],
    pair=old_tokenizer.special_tokens_map["bos_token"] +
    "${0}" + old_tokenizer.special_tokens_map["eos_token"] +
    "${1}" + old_tokenizer.special_tokens_map["eos_token"],
    special_tokens=[
        (old_tokenizer.special_tokens_map["bos_token"], 0),
        (old_tokenizer.special_tokens_map["eos_token"], 1),
        (old_tokenizer.special_tokens_map["unk_token"], 2),
    ],
)

# Train the new tokenizer on the new dataset
special_tokens = list(old_tokenizer.special_tokens_map.values())
trainer = trainers.BpeTrainer(
    vocab_size=old_tokenizer.vocab_size, special_tokens=special_tokens)
tokenizer.train_from_iterator(sentences_iterator, trainer=trainer)

# Save the new tokenizer
new_vocab_file_path = os.path.join(env['weights'], 'vocab', 'new_vocab.txt')
new_merges_file_path = os.path.join(env['weights'], 'vocab', 'new_merges.txt')
tokenizer.model.save(new_vocab_file_path, new_merges_file_path)
