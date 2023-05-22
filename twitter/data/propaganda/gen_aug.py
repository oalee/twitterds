import json
import os
import re
import yerbamate
import ipdb

env = yerbamate.Environment()


# we should have two propaganda paths, arabic, multi language

id = env.get_hparam("id")
p = os.path.join(env["data"], "propaganda", f"combined{id}.json")

data = json.load(open(p))


# the structure is list of dictionaries, each has text and labels
# base on each, generate two instruction, one for predicting the label, one for predicting label and spans


instruction = "Classify propaganda techniques, identify sections for each technique."
instruction2 = "Predict propaganda technique labels separated by comma."

# remove urls and @* and RT from text
print("Length before augmentig two tasks:", len(data))

new_data = []

for item in data:
    if len(item["labels"]) > 0:
        #
        # labels is a list of dictionaries, each has start and end and technique and text
        # add classified {techniqe} from {start} to {end}: {text}
        #

        new_labels = []

        for label in item["labels"]:
            try:
                fragment = (
                    label["text_fragment"]
                    if "text_fragment" in label
                    else label["text"]
                )
                new_labels.append(
                    f"Classified {label['technique']} from {label['start']} to {label['end']}: {fragment}"
                )
            except:
                ipdb.set_trace()

        all_labels = "\n".join(new_labels)

        new_data.append(
            {
                "input": item["text"],
                "output": all_labels,
                "instruction": instruction,
            }
        )

        # second instruciton, predict labels
        # make a set to remove duplicates, but keep the order
        tqs = set()
        for label in item["labels"]:
            tqs.add(label["technique"])





        new_data.append(
            {
                "input": item["text"],
                "output": ", ".join(tqs),
                "instruction": instruction2,
            }
        )

    # no label, predict No propaganda
    else:
        new_data.append(
            {
                "input": item["text"],
                "output": "No propaganda",
                "instruction": instruction,
            }
        )

        new_data.append(
            {
                "input": item["text"],
                "output": "No propaganda",
                "instruction": instruction2,
            }
        )


print("Length after augmentig two tasks:", len(new_data))


json.dump(new_data, open(f"data/propaganda/instruction{id}_aug.json", "w"), indent=4, ensure_ascii=False)
