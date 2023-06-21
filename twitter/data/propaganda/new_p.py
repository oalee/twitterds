import os, yerbamate, ipdb, json


env = yerbamate.Environment()


path = os.path.join(env["local_d"], "propaganda")


# load the data
all_data = json.load(open(os.path.join(path, "all_data_with_spans.json")))
labels = json.load(open(os.path.join(path, "labels_9.json")))

new_2 = json.load(open(os.path.join(path, "exp_labels_2.json")))
new_3 = json.load(open(os.path.join(path, "exp_labels_3.json")))

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


# # add all_data to labels based on id
# for i, label in enumerate(labels):
#     # get the id
#     id = label["id"]
#     # find the data with the same id
#     data = [d for d in all_data if d["id"] == id]

#     try:
#         label["data"] = data[0]
#     except:
#         # merged ids, the data has two ids
#         id_s = id.split("_")
#         data = [d for d in all_data if d["id"] == id_s[0]+ "_" + id_s[1] or d["id"] == id_s[0]+ "_" + id_s[2]]

for i, label in enumerate(labels):
    # get the id
    id = label["id"]
    # find the data with the same id
    data = [d for d in all_data if d["id"] == id]

    try:
        label_data = data[0]
        # concatenate text, labels, and spans
        label["data"] = {
            "text": label_data["text"],
            "labels": label_data["labels"],
            "spans": label_data["spans"],
        }
    except:
        # merged ids, the data has two ids
        id_s = id.split("_")
        data = [
            d
            for d in all_data
            if d["id"] == id_s[0] + "_" + id_s[1] or d["id"] == id_s[0] + "_" + id_s[2]
        ]

        if len(data) > 0:
            # concatenate data for merged ids
            data = sorted(data, key=lambda x: int(x["id"].split("_")[1]))
            concatenated_text = " ".join([d["text"] for d in data])
            concatenated_labels = [label for d in data for label in d["labels"]]
            concatenated_spans = [span for d in data for span in d["spans"]]

            label["data"] = {
                "text": concatenated_text,
                "labels": concatenated_labels,
                "spans": concatenated_spans,
            }


# do the same for old_labels
for i, label in enumerate(old_labels):
    # get the id
    id = label["id"]
    # find the data with the same id
    data = [d for d in all_data if d["id"] == id]

    try:
        label_data = data[0]
        # concatenate text, labels, and spans
        label["data"] = {
            "text": label_data["text"],
            "labels": label_data["labels"],
            "spans": label_data["spans"],
        }
    except:
        # merged ids, the data has two ids
        id_s = id.split("_")
        data = [
            d
            for d in all_data
            if d["id"] == id_s[0] + "_" + id_s[1] or d["id"] == id_s[0] + "_" + id_s[2]
        ]

        if len(data) > 0:
            # concatenate data for merged ids
            data = sorted(data, key=lambda x: int(x["id"].split("_")[1]))
            concatenated_text = " ".join([d["text"] for d in data])
            concatenated_labels = [label for d in data for label in d["labels"]]
            concatenated_spans = [span for d in data for span in d["spans"]]

            label["data"] = {
                "text": concatenated_text,
                "labels": concatenated_labels,
                "spans": concatenated_spans,
            }

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

# now lets create instruction_set
instruction_set = []
for i, label in enumerate(labels):
    text = label["data"]["text"]

    input = text

    output = json.dumps(
        {
            "labels": label["data"]["labels"],
            "explanation": label["explanation"]
        },
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

    output = json.dumps(
        {
            "labels": label["data"]["labels"],
            "explanation": label["explanation"]
        },
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

# # now create instruction set for only labels
# for i, label in enumerate(labels):
#     text = label["data"]["text"]

#     input = text

#     output = json.dumps(
#         {
#             "labels": label["data"]["labels"],
#         },
#         indent=2,
#     )

#     instruction_set.append(
#         {
#             "input": input,
#             "output": output,
#             "instruction": instruction_only_labels,
#         }
#     )

# # now create instruction set for only labels
# for i, label in enumerate(old_labels):
#     text = label["data"]["text"]

#     input = text

#     output = json.dumps(
#         {
#             "labels": label["data"]["labels"],
#         },
#         indent=2,
#     )

#     instruction_set.append(
#         {
#             "input": input,
#             "output": output,
#             "instruction": instruction_only_labels,
#         }
#     )

# ipdb.set_trace() 
# # for ids that have two concated with _ , add both ids
# ids =[ item["id"] for item in  instruction_set]

# # check if there are id_p1_p2 splitted ids in ids then add id_p1 and id_p2
# new_ids = []
# for id in ids:
#     if len(id.split("_")) > 2:
#         new_ids +=  id.split("_")[0]+ "_"+ id.split("_")[1]
#         new_ids +=  id.split("_")[0]+ "_"+ id.split("_")[2]
#     else:
#         new_ids.append(id)

# ids = new_ids



# for ids not in labels, only add labels
for i, item in enumerate(data):
    text = item["text"]

    input = text

    output = json.dumps(
        {
            "labels": item['labels'],
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



# save the instruction set
json.dump(
    instruction_set, open(os.path.join(path, "instruction_set_aug.json"), "w"), indent=4
)
