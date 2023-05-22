import json
import os
import random
import time
import requests, yerbamate

env = yerbamate.Environment()

# data_path = env["data"] / "propaganda" / "mult256.json"


instruction = """
Instruction: Propaganda Explanation Generation

    Task: Your task is to generate an explanation for the propaganda labels provided in the input text. The labels indicate propaganda techniques employed in the text. If there are no labels, provide an explanation for why no propaganda techniques were identified.

    Input: You will be provided with a JSON example containing the following fields:
        "id": An identifier for the example.
        "text": The input text to be analyzed for propaganda techniques. It can be in any language.
        "labels": A list of propaganda labels associated with the text. Each label contains the propaganda technique name, the corresponding text fragment, and the start and end indices of the fragment within the text.

    IMORTANT Output: Your model should perform the following:
        Generate an explanation for the propaganda labels present in the input text. If there are no labels, provide an explanation for why no propaganda techniques were identified.
        Format the output as a JSON object containing the following fields:
            "id": The identifier for the example.
            "explanation": Propaganda labels in the text and A single explanation section describing or exemplifying how each propaganda technique is employed in the text. If there are no labels, provide an explanation stating that no propaganda techniques were identified and the reasons behind it.

    Format: Please provide the output as a JSON object adhering to the structure described above.

Please note that you will receive a JSON example as input. Your model should generate an explanation for the propaganda labels in the text or provide an explanation for why no propaganda techniques were identified. The output JSON should include the necessary information, including the explanation.

"""

instruction = """
Your Role and Instruction: Propaganda Explanation Generator

Description:
Given a JSON example containing an identifier, input text, and a list of propaganda labels associated with the text, the task of this role is to generate an explanation for the propaganda techniques identified in the input text or provide an explanation for why no propaganda techniques were identified. 


Instructions:

    Read the provided JSON example containing the following fields: "id", "text", and "labels".

    Check if there are any propaganda labels present in the "labels" field.

    If there are propaganda labels:
    a. Generate an explanation for each propaganda technique identified in the text.
    b. Formulate a comprehensive explanation or provide an exemplification of how each propaganda technique is employed in the text.
    c. Include the explanation for each propaganda label in the output JSON.
    

    If there are no propaganda labels:
    a. Provide an explanation stating that no propaganda techniques were identified in the text.
    b. Include the reasons behind the lack of identification in the output JSON.

    Format the output as a JSON object with the following fields:
    a. "id": The identifier for the example.
    b. "explanation": Propaganda labels and the number of propaganda technique lables, and a single explanation section describing or exemplifying how each propaganda technique is employed in the text. If there are no labels, provide an explanation stating that no propaganda techniques were identified and the reasons behind it.

    Return the output JSON object as the result.
    Remember to return the output as JSON and one single explanation section for all propaganda labels as a string.
    IMORTANT: In case there are no propaganda lables The reason cannot be absence of labels in input, it should be the reason why it was not identified by explaning what does the input means. Do no mention the absence of labels in the input in the explanation. Explain the input and why it is not propaganda.
    IMORTANT: In case there are propaganda labels, first say write the number and list all the propganda lables seperated by comma or and, then explain the reasoning. 
"""


def sent_message(message):
    # delay between 0.5 and 1.5 seconds
    time.sleep(random.random() + 0.5)

    response = requests.post(
        "http://localhost:8766/v1/chat/completions",
        json={
            "messages": message,
            "model": "gpt-3.5-turbo",
            "newChat": False,
            "temperature": 0,
            "max_tokens": 1544,
        },
    ).json()
    return response["choices"][0]["message"]["content"]


sent_message(instruction)

data_path = os.path.join(env["data"], "propaganda", "mult256.json")
data = json.load(open(data_path))

# load responsesz.json if exists, and start from there
if os.path.exists("responsesz.json"):
    responses = json.load(open("responsesz.json"))

    # deduplicate responses
    # responses = list({v["id"]: v for v in responses}.values())
    # import ipdb
    # ipdb.set_trace()

    # parse item from string to json with try except and json.loads
    # for i, item in enumerate(responses):
    #     try:
    #         responses[i] = json.loads(item)
    #     except:
    #         pass

else:
    responses = []
# import ipdb
# ipdb.set_trace()

counter = 0

# responses = []
for i, item in enumerate(data):
    # if very small, skip
    if len(item["text"]) < 35 and len(item["labels"]) == 0:
        continue

    # if already done, skip
    # check if id is in responses
    if any([item["id"] == r["id"] for r in responses if "id" in r]):
        continue

    # format item to json string
    item_j = json.dumps(item, ensure_ascii=False, indent=4)

    # send it to the server

    if counter % 10 == 0:
        item_j = instruction + "\n" + item_j

    response = sent_message(item_j)

    # append the response to responses
    try:
        responses.append(json.loads(response))
    except:
        print("ERROR IN RESPONSE")

        response = sent_message(instruction + "\n" + item_j)

        # try:
        #     j = json.loads(response)
        #     responses.append(j)
        # except:
        #     pass

        time.sleep(1)

        # response = sent_message(item_j)

        try:
            responses.append(json.loads(response))
        except:
            print("PANIC ERROR PASSING")
            continue

    counter += 1

    # response = json.loads(item_j)

    # responses.append(response)

    print("counter", counter)
    # save responses to a file
    json.dump(responses, open("responsesz.json", "w"), indent=4)

    # sleep for 0.1 to .3 seconds
    # time.sleep(random.uniform(1.0, 1.5))

    # every 30 steps, give the role instructions
    # if counter % 10 == 0:
    #     # send the instruction to the server
    #     sent_message(instruction + "\n" + "THIS IS A REMINDER, YOU ARE DOING GREAT!, KEEP GOING! AND REPLY TO THIS MESSAGE WITH 'Nice :x' and lets CONTINUE")


# import json
