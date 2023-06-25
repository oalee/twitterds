import os, yerbamate, ipdb, json


env = yerbamate.Environment()


path = os.path.join(env["local_d"], "propaganda")


# load the data
all_data = json.load(open(os.path.join(path, "all_data_with_spans.json")))
labels = json.load(open(os.path.join(path, "labels.json")))

new_2 = json.load(open(os.path.join(path, "exp_labels_2.json")))
new_3 = json.load(open(os.path.join(path, "exp_labels_3.json")))

# exp_labels_comprehensive_{i}.json i being 1, 3,4,5,6
comprehensive_file_names = [
    "exp_labels_comprehensive_1.json",
    "exp_labels_comprehensive_3.json",
    "exp_labels_comprehensive_4.json",
    "exp_labels_comprehensive_5.json",
    "exp_labels_comprehensive_6.json",
]

comprehensive_labels = [
    json.load(open(os.path.join(path, file_name)))
    for file_name in comprehensive_file_names
]

# concatenate all comprehensive labels
comprehensive_labels = [label for labels in comprehensive_labels for label in labels]

def find_item_by_id(id):
    # if id has multiple '_', we need to merge, otherwise, just look for the id in all_data
    id_s = id.split("_")
    if len(id_s) > 2:
        # check how many ids are in all_data, format is article id, paragraph id, paragraph id2,...

        data = [
            d
            for d in all_data
            if d["id"] == id_s[0] + "_" + id_s[1] or d["id"] == id_s[0] + "_" + id_s[2]
        ]

        # could be three paragraph ids
        if len(id_s) > 4:
            data = [
                d
                for d in all_data
                if d["id"] == id_s[0] + "_" + id_s[1]
                or d["id"] == id_s[0] + "_" + id_s[2]
                or d["id"] == id_s[0] + "_" + id_s[3]
            ]

        text = "\n".join([d["text"] for d in data])

    else:
        text = [d["text"] for d in all_data if d["id"] == id][0]

    return text

index = 0
# find the first index that has new = true
for i, label in enumerate(labels):
    if "new" in label and label["new"] == True:
        # ipdb.set_trace()
        index = i
        break

# from labels 5660 to end (discard the first 5660)
old_labels = labels[:index]
labels = labels[index:]
labels = labels + new_2 + new_3


def find_ritem_by_id(id):
    # if id has multiple '_', we need to merge, otherwise, just look for the id in all_data
    id_s = id.split("_")
    if len(id_s) > 2:
        # check how many ids are in all_data, format is article id, paragraph id, paragraph id2,...

        data = [
            d
            for d in all_data
            if d["id"] == id_s[0] + "_" + id_s[1] or d["id"] == id_s[0] + "_" + id_s[2]
        ]

        # could be three paragraph ids
        if len(id_s) > 4:
            data = [
                d
                for d in all_data
                if d["id"] == id_s[0] + "_" + id_s[1]
                or d["id"] == id_s[0] + "_" + id_s[2]
                or d["id"] == id_s[0] + "_" + id_s[3]
            ]

        text = "\n".join([d["text"] for d in data])
        # concatenate labels
        labels = []
        for d in data:
            labels += d["labels"]

        # remove duplicates
        labels = list(set(labels))

        # concatenate spans
        spans = []
        for d in data:
            spans += d["spans"]

        return {"text": text, "labels": labels, "spans": spans}

    else:
        text = [d["text"] for d in all_data if d["id"] == id][0]
        labels = [d["labels"] for d in all_data if d["id"] == id][0]
        spans = [d["spans"] for d in all_data if d["id"] == id][0]

        return {"text": text, "labels": labels, "spans": spans}

    return text


for i, label in enumerate(labels):
    # get the id
    id = label["id"]
    # find the data with the same id
    data = [d for d in all_data if d["id"] == id]
    real_data = find_ritem_by_id(id)

    # check if real_data has only one Repitition span in spans then skip 
    # if len(real_data["spans"]) == 1 and real_data["spans"][0]["technique"] == "Repetition":
    #     continue

    label["data"] = real_data

    # try:
    #     label_data = data[0]
    #     # concatenate text, labels, and spans
    #     label["data"] = {
    #         "text": label_data["text"],
    #         "labels": label_data["labels"],
    #         "spans": label_data["spans"],
    #     }
    # except:
    #     # merged ids, the data has two ids
    #     id_s = id.split("_")
    #     data = [
    #         d
    #         for d in all_data
    #         if d["id"] == id_s[0] + "_" + id_s[1] or d["id"] == id_s[0] + "_" + id_s[2]
    #     ]

    #     if len(data) > 0:
    #         # concatenate data for merged ids
    #         data = sorted(data, key=lambda x: int(x["id"].split("_")[1]))
    #         concatenated_text = " ".join([d["text"] for d in data])
    #         concatenated_labels = [label for d in data for label in d["labels"]]
    #         concatenated_spans = [span for d in data for span in d["spans"]]

    #         label["data"] = {
    #             "text": concatenated_text,
    #             "labels": concatenated_labels,
    #             "spans": concatenated_spans,
    #         }


# do the same for old_labels
for i, label in enumerate(old_labels):
    # get the id
    id = label["id"]
    # find the data with the same id
    data = [d for d in all_data if d["id"] == id]

   
    real_data = find_ritem_by_id(id)    

    # check if real_data has only one Repitition span in spans then skip
    # if len(real_data["spans"]) == 1 and real_data["spans"][0]["technique"] == "Repetition":
    #     continue

    label["data"] = real_data

# now we need to make instruction-set for training


instruction = """
Role: Propaganda Analysis

Task:

    Analyze text for propaganda techniques.
    If found, create an explanation including usage and targets.
    If not, explain why.
    Return a JSON object with "labels" and "explanation".
"""

instruction_only_explanation = """
Role: Propaganda Analysis

Task:

    Analyze text for propaganda techniques.
    If found, create a detailed explanation.
    If not, explain why.
    Return a JSON object with "labels" and "explanation".
"""


instruction_only_labels = """
Role: Propaganda Classifier

Task:

    Analyze text for propaganda techniques.
    Return a JSON object with "labels".
"""

instruction_comprehensive_in_depth = """
Role: Propaganda Analysis

Task:

    Analyze text for propaganda techniques.
    If found, create an comprehensive and in-depth explanation of the technique(s).
    If not, create an comprehensive and in-depth explanation of why the text is not propaganda.

    Return a JSON object with "labels" and "explanation".
"""
# now lets create instruction_set
instruction_set = []
for i, label in enumerate(labels):
    text = label["data"]["text"]

    input = text

    r_item = find_ritem_by_id(label["id"])
    # check if the item has repetition and len of spans is 1
    if (
        "Repetition" in r_item["labels"]
        and len([span for span in r_item["spans"] if span["technique"] == "Repetition"])
        == 1
    ):
        continue

    output = json.dumps(
        {"labels": label["data"]["labels"], "explanation": label["explanation"]},
        indent=2,
    )

    instruction_set.append(
        {
            "input": input,
            "output": output,
            "instruction": instruction,
            "id": label["id"],
        }
    )

# old labels don't have target
for i, label in enumerate(old_labels):
    text = label["data"]["text"]

    input = text

    r_item = find_ritem_by_id(label["id"])

    # check if the item has repetition and len of spans is 1
    if (
        "Repetition" in r_item["labels"]
        and len([span for span in r_item["spans"] if span["technique"] == "Repetition"])
        == 1
    ):
        continue

    output = json.dumps(
        {"labels": label["data"]["labels"], "explanation": label["explanation"]},
        indent=2,
    )

    instruction_set.append(
        {
            "input": input,
            "output": output,
            "instruction": instruction_only_explanation,
            "id": label["id"],
        }
    )


# for ids not in labels, only add labels
for i, item in enumerate(all_data):
    text = item["text"]

    input = text

    # check if technique contains "Repetition" and Reptition count in spans is 1, skip
    if (
        "Repetition" in item["labels"]
        and len([span for span in item["spans"] if span["technique"] == "Repetition"])
        == 1
    ):
        continue

    output = json.dumps(
        {
            "labels": item["labels"],
        },
        indent=2,
    )

    instruction_set.append(
        {
            "input": input,
            "output": output,
            "instruction": instruction_only_labels,
        }
    )





for item in comprehensive_labels:
    # find item by id

    input = find_item_by_id(item["id"])

    output = json.dumps(
        {
            "labels": item["labels"],
            "explanation": item["explanation"],
        },
        indent=2,
    )

    instruction_set.append(
        {
            "input": input,
            "output": output,
            "instruction": instruction_comprehensive_in_depth,
        }
    )


# save the instruction set
json.dump(
    instruction_set, open(os.path.join(path, "instruction_set_aug_2.json"), "w"), indent=4
)
