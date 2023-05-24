import os, yerbamate, json, re, ipdb


env = yerbamate.Environment()


# mult256
data_path = os.path.join(env["data"], "propaganda", "mult256.json")
data = json.load(open(data_path))

# label path ./labels.json

label_path = os.path.join("labels.json")
labels = json.load(open(label_path))

# we need to add the labels to the data

# merge the labels with the data by id

new_data = []
for item in labels:
    # data is list, we need to find the item with the same id
    # find the data entry with the same id

    data_entry = list(filter(lambda x: x["id"] == item["id"], data))

    # data_entry = data.find(lambda x: x["id"] == item["id"])

    # if more than one data entry is found, log it
    if len(data_entry) > 1:
        print("more than one data entry found for id", item["id"])

        ipdb.set_trace()

    if type(item["explanation"]) == list:
        # ipdb.set_trace()
        # map a function to the list
        # map(lambda x: x["explanation"], item["explanation"])

        item["explanation"] = "\n".join(
            map(lambda x: x["explanation"], item["explanation"])
        )

    new_data.append({**data_entry[0], **item})


save_path = os.path.join(env["data"], "propaganda", "mult256_expln.json")
# save the new data
json.dump(new_data, open(save_path, "w"), indent=4, ensure_ascii=False)


# now generate instructions for the annotators
instruction_1 = """Classify propaganda techniques in the following text. Only predict labels, do not explain the reason for the prediction."""

instruction_2 = """Classify propaganda techniques in the following text and explain the reason for the prediction. The explanation should be comprehensive."""
print(len(new_data), "items in the data")
# generate the instructions
instructions = []
for item in new_data:
    output_simple = (
        "No propaganda technique found in the input text."
        if len(item["labels"]) == 0
        else ", ".join(set(map(lambda x: x["technique"], item["labels"])))
    )
    if len(item["labels"]) == 1:
        output_simple = item["labels"][0]["technique"]
        output_simple = f"Identified propaganda technique: {output_simple}"
    else:
        output_simple = f"{output_simple}"

    instructions.append(
        {
            "id": item["id"],
            "input": item["text"],
            "instruction": instruction_1,
            "output": output_simple,
        }
    )
    instructions.append(
        {
            "id": item["id"],
            "input": item["text"],
            "instruction": instruction_2,
            "output": item["explanation"],
        }
    )

# save the instructions


save_path = os.path.join(env["data"], "propaganda", "mult256_expln_instructions.json")
json.dump(instructions, open(save_path, "w"), indent=4, ensure_ascii=False)
