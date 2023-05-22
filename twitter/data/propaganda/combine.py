import json
import os
import re
import yerbamate
import ipdb

env = yerbamate.Environment()


# we should have two propaganda paths, arabic, multi language

id = env.get_hparam("id", 256)

arabic_path = os.path.join(env["data"], "propaganda", "arabic.json")
multi_path = os.path.join(env["data"], "propaganda", f"mult{id}.json")


# the structure is list of dictionaries, each has text and labels
# labels is a list of dictionaries, each has start and end and technique


arabic = json.load(open(arabic_path))
multi = json.load(open(multi_path))


# the labels are slightly different, create a set of both to join them


arabic_labels = set()
multi_labels = set()


for item in arabic:
    for label in item["labels"]:
        arabic_labels.add(label["technique"])

for item in multi:
    for label in item["labels"]:
        multi_labels.add(label["technique"])

print("Arabic labels:\n")
print(arabic_labels)

print("Multi labels:\n")
print(multi_labels)


# Now combine the two and map them to a single label set

label_mapping = {
    "Loaded Language": "Loaded_Language",
    "Exaggeration/Minimisation": "Exaggeration-Minimisation",
    "Flag-waving": "Flag_Waving",
    "Obfuscation, Intentional vagueness, Confusion": "Obfuscation-Vagueness-Confusion",
    "Name calling/Labeling": "Name_Calling-Labeling",
    "Black-and-white Fallacy/Dictatorship": "False_Dilemma-No_Choice",
    "Slogans": "Slogans",
    "Appeal to fear/prejudice": "Appeal_to_Fear-Prejudice",
    "Appeal to authority": "Appeal_to_Authority",
    "Repetition": "Repetition",
    "Doubt": "Doubt",
    "Causal Oversimplification": "Causal_Oversimplification",
    "Thought-terminating cliché": "Conversation_Killer",
    "Smears": "Name_Calling-Labeling",  # Considered as a type of name-calling/labeling
    "Whataboutism": "Whataboutism",
    "Glittering generalities (Virtue)": "Appeal_to_Values",  # Similar to appeal to values
    "Presenting Irrelevant Data (Red Herring)": "Red_Herring",
}


def clean_text(text):  # remove urls and @* and RT from text
    text = re.sub(r"http\S+", "", text, flags=re.MULTILINE)  # remove urls
    # doesnt removes https://t.co/1Qm0zYJQ2b

    text = re.sub(r"https\S+", "", text, flags=re.MULTILINE)  # remove urls

    # https://t.co/baZCMHiq0U this is not removed why?
    # ipdb.set_trace()

    text = re.sub(r"@\S+", "", text, flags=re.MULTILINE)  # remove @*
    text = re.sub(r"RT", "", text).strip()  # remove RT
    # text = re.sub(r"\n", "", text)
    return text


# First, let's apply the label mapping to the arabic dataset
n = []
for item in arabic:
    # for label in item["labels"]:
    #     # replace the original label with the mapped label

    # clean the text
    item["text"] = clean_text(item["text"])
    # update label start and end positions
    # ipdb.set_trace()
    for label in item["labels"]:
        id = "text" if "text" in label else "text_fragment"
        label["start"] = item["text"].find(label[id])
        label["end"] = label["start"] + len(label[id])
        label["technique"] = label_mapping[label["technique"]]

    n.append(item.copy())

# ipdb.set_trace()
arabic = n.copy()

# ipdb.set_trace()

# ipdb.set_trace()
# Then, do the same for the multi dataset
# for item in multi:
#     for label in item["labels"]:

#         label["technique"] = label_mapping[label["technique"]]

# all_labels = set()

# for item in arabic:
#     for label in item["labels"]:
#         all_labels.add(label["technique"])

# for item in multi:
#     for label in item["labels"]:
#         all_labels.add(label["technique"])

# print("All labels:\n")
# print(all_labels)

# Now, combine the two datasets into one
combined_dataset = n + multi

# ipdb.set_trace()
print("Arabic dataset length: ", len(arabic))
print("Multi dataset length: ", len(multi))

print("Combined dataset length: ", len(combined_dataset))


# save the combined dataset to a json file
with open(os.path.join(env["data"], "propaganda", f"combined{env['id']}.json"), "w") as f:
    json.dump(combined_dataset, f, indent=4, ensure_ascii=False)
