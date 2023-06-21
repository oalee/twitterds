prompt_template = """
You will be provided with the following information:

    An arbitrary text sample. The sample is delimited with triple backticks.
    List of categories the text sample can be assigned to. The list is delimited with square brackets. The categories in the list are enclosed in the single quotes and comma separated.
    The name of the entity for which you are to analyze the sentiment in the text.

Perform the following tasks:

    Identify the sentiment towards the given entity in the provided text.
    Assign the provided text to the category that best describes this sentiment.
    Provide an explanation for your categorization.
    Provide your response in a JSON format containing two keys: label and explanation. The value of the label should correspond to the assigned category, and the value of the explanation should be the rationale behind this assignment.

List of categories: ['positive', 'negative', 'neutral']

Text sample: '''{text}'''


Entity: "{entity}"

Your JSON response:
"""

re_prompt_template = """
Please re-evaluate the text sample with a focus on the sentiment expressed towards the specified entity. Ensure to consider nuances that might have been missed initially, such as:

1. Contextual Understanding: Consider the overall context in which the text is written to understand the underlying sentiment.

2. Language Nuances: Pay attention to any language nuances including sarcasm, irony, or undermining language that might change the sentiment.

3. Emotional Expressions: Observe if there are any explicit emotional expressions towards the entity that can influence the sentiment.

4. Cultural References: Reflect on any cultural references that may impact the sentiment expressed in the text.

Remember that the objective is to identify the sentiment towards the given entity and categorize it as 'positive', 'negative', or 'neutral'. Provide your re-assessment in a JSON format with two keys: 'label' and 'explanation'. The 'label' should represent the sentiment category and the 'explanation' should detail the reasons for this assignment. Keep your explanation concise and directly relevant to the sentiment towards the entity.

Your JSON response:
"""

re_two_prompt_template = """
Please re-evaluate the sentiment towards the specified entity in the text by comparing your previous outputs. Emphasize identifying the less biased and more objective analysis. Consider:

1. Consistency: Are there consistent findings in the previous outputs?

2. Contradictions: Resolve any contradictions between the previous outputs.

3. Bias Assessment: Evaluate each analysis for potential biases and determine which one is more objective.

4. Evidence in Text: Base your final assessment on the evidence within the text.

Provide your final response in JSON format with two keys: 'label' and 'explanation'. The 'label' should represent the sentiment category, and the 'explanation' must solely focus on explaining the sentiment towards the entity based on the text. IMPORTANT Do not mention the process of comparison or re-evaluation in the explanation.


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

h_path = os.path.join(env["local_d"], "propaganda", "hamed.json")

import json

# this is jsonl, read it line by line
with open(h_path) as f:
    ds = [json.loads(line) for line in f]

entity = "Hamed Esmailion"

s_path = os.path.join(env["local_d"], "propaganda", "hamed_sentiment.json")

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

    text = item["rawContent"]
    prompt = prompt_template.format(text=text, entity=entity)
    print(prompt)

    model.start_new_chat()
    response_list = []

    while True:
        try:
            label = model.GetAnswer(prompt)
            # check if valid json
            
            print(prompt)
            
            print(label)

            ll = json.loads(label)
            print(ll)

            response_list.append(ll)
            if len(response_list) == 1:
                prompt = re_prompt_template
                continue
            elif len(response_list) == 2:
                prompt = re_two_prompt_template
                continue
            break

        except:
            print("Error, trying again...")
            time.sleep(10)

    # label = model.send_prompt(prompt)
    item["label"] = {entity: response_list}

    labled.append(item)

    with open(s_path, "w") as f:
        json.dump(labled, f, ensure_ascii=False, indent=4)
