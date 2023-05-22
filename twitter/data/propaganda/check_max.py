import json
import os
import yerbamate
import ipdb

env = yerbamate.Environment()


# we should have two propaganda paths, arabic, multi language

import re


from transformers import LlamaTokenizer


tokenizer = LlamaTokenizer.from_pretrained(
    "decapoda-research/llama-7b-hf", add_eos_token=True
)
tokenizer.pad_token = tokenizer.eos_token
tokenizer.pad_token_id = tokenizer.eos_token_id

# data = load_dataset("json", data_files="/home/al/GitHub/twitter/data/propaganda/combined_aug.json")


def generate_prompt(data_point):
    # sorry about the formatting disaster gotta move fast
    if data_point["input"]:
        return f"""
        
### Instruction:
{data_point["instruction"]}
{data_point["input"]}

### Response:
{data_point["output"]}"""
    else:
        return f"""
        
### Instruction:
{data_point["instruction"]}

### Response:
{data_point["output"]}"""


# check max token length of data

id = env.get_hparam("id")

p = os.path.join(env["data"], "propaganda", f"instruction{id}_aug.json")

data = json.load(open(p))


lens = [len(tokenizer.encode(generate_prompt(x))) for x in data]

# find max length

print(max(lens))
