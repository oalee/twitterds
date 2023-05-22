import json
import os
import yerbamate
import ipdb

env = yerbamate.Environment()


# we should have two propaganda paths, arabic, multi language

import re


p = os.path.join(env["data"], "propaganda", "multi.json")

data = json.load(open(p))


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


def split_long_prompts(data, max_length=1024):
    """
    Splits long texts into smaller ones while preserving labels' positions.

    Arguments:
    data -- List of dictionary each containing 'text' and 'labels'
    max_length -- maximum allowed token count for each text
    """

    new_data = []
    for item in data:
        # tokenizing input to check length
        labl_str = "Classified From X to Y ".join(
            [x["text_fragment"] for x in item["labels"]]
        )
        tokens = tokenizer.encode(item["text"] + labl_str, truncation=False)

        if len(tokens) > max_length:
            # the rough length of each chunk in characters
            rough_chunk_len = len(item["text"]) * max_length // len(tokens)

            # Create new items
            start_char = 0
            while start_char < len(item["text"]):
                end_char = start_char + rough_chunk_len

                # adjust end_char to the nearest dot
                # adjust end_char to the nearest dot followed by a whitespace or the end of the text
                while end_char < len(item["text"]) and not (
                    item["text"][end_char] == "."
                    and (
                        end_char + 1 == len(item["text"])
                        or item["text"][end_char + 1].isspace()
                    )
                ):
                    end_char += 1

                # if there's no dot after start_char, just split at rough_chunk_len
                if end_char == len(item["text"]):
                    end_char = min(start_char + rough_chunk_len, len(item["text"]))

                # Extend the current chunk to include the whole label that would have been split
                for label in item["labels"]:
                    if (
                        label["start"] >= start_char
                        and label["start"] < end_char
                        and label["end"] > end_char
                    ):
                        end_char = label["end"]

                # adjusting labels' positions
                new_labels = []
                for label in item["labels"]:
                    # ensure the entire label fragment is inside the chunk
                    if label["end"] <= start_char or label["start"] >= end_char:
                        # if label's span is completely outside this chunk, skip it
                        continue
                    if label["start"] >= start_char and label["end"] <= end_char:
                        new_label = label.copy()
                        # splitting original input text
                        new_text = item["text"][
                            start_char : end_char + 1
                        ].strip()  # remove leading and trailing spaces/newlines
                        new_label["start"] = new_text.find(label["text_fragment"])
                        new_label["end"] = new_label["start"] + len(
                            label["text_fragment"]
                        )
                        new_labels.append(new_label)

                new_item = {"text": new_text, "labels": new_labels}

                new_data.append(new_item)

                # update start_char for next iteration
                start_char = end_char + 1
        else:
            new_data.append(item)

    return new_data


max_length = env.get_hparam("max_length", 512)

print("original length", len(data))

new_data = split_long_prompts(data, max_length=max_length)

print(len(new_data))

# add ids to new data
for i, item in enumerate(new_data):
    item["id"] = i

json.dump(new_data, open(f"data/propaganda/mult{max_length}.json", "w"), indent=4)
