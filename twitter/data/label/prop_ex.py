prompt_template = """
Instructions:

Read the provided JSON example containing the following fields: "id", "text", and "labels".
Check if there are any propaganda labels present in the "labels" field.
If there are propaganda labels:
a. Generate an explanation for each propaganda technique present in the text.
b. Formulate a comprehensive explanation or provide exemplification of how each propaganda technique is employed in the text, including the identified targets and objectives of the propaganda.
c. Include the explanation, targets, and objectives for each propaganda label in the output JSON.
If there are no propaganda labels:
a. Analyze the content of the text to identify any persuasive techniques or rhetoric that may be present, even if they don't align with specific propaganda labels.
b. Provide an explanation for the persuasive techniques employed in the text and their potential effects on the audience.
c. Identify the targets and objectives presented in the text without referring explicitly to propaganda techniques.
d. Additionally, explain that while no specific propaganda labels were identified, the text employs persuasive techniques to influence the audience's perception and decision-making.
Format the output as a JSON object with the following fields:
a. "id": The identifier for the example.
b. "explanation": A single explanation section describing or exemplifying how each propaganda technique is employed in the text, including the identified targets and objectives of the propaganda, and the explanations. If there are no labels, provide an explanation for the persuasive techniques employed in the text, the identified targets and objectives, and the rationale for the absence of specific propaganda labels.
c. "labels": A list of identified labels of propaganda, if any.
Return the output JSON object as the result.
Remember to return the output as JSON and provide one single explanation section for all propaganda labels, targets, objectives, and the rationale for the absence of specific propaganda labels as a string.

Text sample: '''{text}'''

Your JSON response:
"""


import os
import time

import tqdm
from .model import Model

model = Model()


from yerbamate import Environment

env = Environment()

# env['local_d'] / propganda/hamed.json

h_path = os.path.join(env["local_d"], "propaganda", "all_data_with_spans.json")

import json

ds = json.load(open(h_path))

s_path = os.path.join(env["local_d"], "propaganda", "p_ex.json")

# if not exist, create it with empty list
if not os.path.exists(s_path):
    with open(s_path, "w") as f:
        json.dump([], f)

with open(s_path) as f:
    labled = json.load(f)

for item in tqdm.tqdm(ds):
    # check if already labeled, based on id
    if item["id"] in [i["id"] for i in labled]:
        continue
    
    t = item["text"]

    if len(t) < 55:
        continue

    # {
    #     "language": "po",
    #     "id": "25125_15",
    #     "text": "Według właścicieli szklarni proceder trwa od dawna i niestraszna mu nawet wojna. Rosyjskie ogórki sprowadzane są tirami do polskich dużych przedsiębiorstw, które pakują je, przyklejają etykietę i puszczają dalej jako swoje, a co ważniejsze dla konsumenta – nasze. Spolszczone rosyjskie ogórki sprzedawane są dużym marketom, ale także małym warzywniakom i przedsiębiorcom na ryneczkach. Importuje się ich tyle, że brakuje miejsca na rodzime warzywa. Sytuacja jest tak poważna, że kupienie polskiego ogórka w Polsce jest praktycznie niemożliwe. Ponieważ cała zabawa jest nielegalna,",
    #     "labels": [
    #         "Appeal_to_Hypocrisy",
    #         "Flag_Waving",
    #         "Name_Calling-Labeling"
    #     ],
    #     "spans": [
    #         {
    #             "technique": "Appeal_to_Hypocrisy",
    #             "text": "Rosyjskie ogórki sprowadzane są tirami do polskich dużych przedsiębiorstw, które pakują je, przyklejają etykietę i puszczają dalej jako swoje, a co ważniejsze dla konsumenta – nasz"
    #         },
    #         {
    #             "technique": "Name_Calling-Labeling",
    #             "text": "Spolszczone rosyjskie"
    #         },
    #         {
    #             "technique": "Flag_Waving",
    #             "text": "kupienie polskiego ogórka w Polsce jest praktycznie niemożliw"
    #         },
    #         {
    #             "technique": "Flag_Waving",
    #             "text": "Importuje się ich tyle, że brakuje miejsca na rodzime warzyw"
    #         }
    #     ]
    # },
    
    # if labels has "repetition" and the spans with repetition are less than two, skip
    if "Repetition" in item["labels"]:
        if len([i for i in item["spans"] if i["technique"] == "Repetition"]) < 2:
            continue

    
    # remove language from item and format object to string from json

    item = {k: v for k, v in item.items() if k != "language"}
    
    text = json.dumps(item, ensure_ascii=False, indent=2)
    
    prompt = prompt_template.format(text=text)
    print(prompt)

    model.start_new_chat()
    response_list = []

    while True:
        try:
            label = model.GetAnswer(prompt)
            # check if valid json
        
            print(label)
            ll = json.loads(label)
            break

        except:
            print("Error, trying again...")
            time.sleep(10)
            model.start_new_chat()

    # label = model.send_prompt(prompt)
    
    labled.append(ll)

    with open(s_path, "w") as f:
        json.dump(labled, f, ensure_ascii=False, indent=4)
