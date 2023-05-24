
import { ChatGPTAPI } from 'chatgpt'

import { ChatGPTUnofficialProxyAPI } from 'chatgpt'

import { writeFile } from 'fs/promises';

// const api = new ChatGPTAPI({ apiKey: process.env.OPENAI_API_KEY })

const api = new ChatGPTUnofficialProxyAPI({
    accessToken: process.env.OPENAI_ACCESS_TOKEN,
    apiReverseProxyUrl: 'https://api.pawan.krd/backend-api/conversation'
})
var instruction = `
Your Role and Instruction: Propaganda Explanation Generator

Description of the task:
Given a JSON example containing an identifier, input text, and a list of propaganda labels associated with the text, the task of this role is to generate an explanation for the propaganda techniques identified in the input text or provide an explanation for why no propaganda techniques were identified. 


Instructions:

    Read the provided JSON example containing the following fields: "id", "text", and "labels".

    Check if there are any propaganda labels present in the "labels" field.

    If there are propaganda labels:
    a. For each propaganda label, find the best segment that matches the labeled propaganda technique in the text.
    b. Generate a explanation for each propaganda technique identified in the text.
    c. Formulate a comprehensive explanation or provide an exemplification of how each propaganda technique is employed in the text.
    d. Include the explanation for each propaganda label in the output JSON.
    

    If there are no propaganda labels:
    a. Provide an explanation stating that no propaganda techniques were identified in the text.
    b. Include the reasons behind the lack of identification in the output JSON.

    Format the output as a JSON object with the following fields:
    a. "id": The identifier for the example.
    b. "explanation": Propaganda labels and the number of propaganda technique lables, and a single explanation section describing or exemplifying how each propaganda technique is employed in the text. If there are no labels, provide an explanation stating that no propaganda techniques were identified and the reasons behind it.

    Return the output JSON object as the result.
    Remember to return the output as JSON and one single explanation section for all propaganda labels as a string.
    IMORTANT: In case there are no propaganda lables The reason cannot be absence of labels in input, it should be the reason why it was not identified by explaning what does the input means. Do no mention the absence of labels in the input in the explanation. Explain the input and why it is not propaganda.
    IMORTANT: In case there are propaganda labels, first say write the exact number and write all the propaganda labels separated by comma or and, then explain the reasoning.
    Reply with '...' if you understand the instructions and are ready to start the task.
`


instruction = `
Your Role and Instruction: Propaganda Explanation Generator

Description of the task:
Given a JSON example containing an identifier, input text, and a list of propaganda labels associated with the text, the task of this role is to generate an explanation for the propaganda techniques identified in the input text or provide an explanation for why no propaganda techniques were identified. 


Instructions:

    Read the provided JSON example containing the following fields: "id", "text", and "labels".

    Check if there are any propaganda labels present in the "labels" field.

    If there are propaganda labels:
    a. For each propaganda label, find the best segment that matches the labeled propaganda technique in the text.
    b. Generate a explanation for each propaganda technique identified in the text.
    c. Formulate a comprehensive explanation or provide an exemplification of how each propaganda technique is employed in the text.
    d. Include the explanation for each propaganda label in the output JSON.
    

    If there are no propaganda labels:
    a. Provide an explanation stating that no propaganda techniques were identified in the text.
    b. Include the reasons behind the lack of identification in the output JSON.

    Format the output as a JSON object with the following fields:
    a. "id": The identifier for the example.
    b. "explanation": Propaganda labels and the number of propaganda technique lables, and a single explanation section describing or exemplifying how each propaganda technique is employed in the text. If there are no labels, provide an explanation stating that no propaganda techniques were identified and the reasons behind it.

    Return the output JSON object as the result.
    Remember to return the output as JSON and one single explanation section for all propaganda labels as a string.
    IMPORTANT: If there are propaganda labels in the input, do not explain there is no propaganda.
    IMPORTANT: In case there are no propaganda labels The reason cannot be absence of labels in input, it should be the reason why it was not identified by explaining what does the input means.
    IMPORTANT: Do not mention the absence of labels in the input as rationale for the explanation. Explain the input and why it is not propaganda. 
    IMPORTANT: In case there are propaganda labels, first say write the exact number and write all the propaganda labels separated by comma or and, then explain the reasoning.
    Reply with '...' if you understand the instructions and are ready to start the task.
    `

var conversationId = '919e681a-5e29-4bfd-bfc0-df879331b315' //'0884c404-7b2c-4f5d-8892-9debd25e6ace' //'9e2b7130-3122-4c1f-b8d8-2de9557e2a21' //'0c307f20-684f-4192-8f9d-29a324d8a22e'//'3f76f671-227a-451e-807f-f142a8ba17c7' //'db5522d9-618f-48e8-b94a-230ea0a04072'
var parentMessageId = 'cc018b3c-e855-4b04-8497-6ed4edde0c2b' //'9a69ae12-aa57-4f2c-b30d-9487434c8f5c'//'486f4206-0762-40ed-8cbf-1860a284c780' //486cb85f-ff17-44bc-ac6a-7c20da20457f' //'4e07b2c3-3f7c-4b28-bf56-8774b7173de3' //'a3039cf9-d94b-4b92-926a-668b26890e4f'

async function send_message(message) {
    // send a message, if res is not null, send a follow-up


    return await api.sendMessage(
        message,
        {
            parentMessageId: parentMessageId,
            model: 'gpt-3.5-turbo',
            conversationId: conversationId,
            timeoutMs: 2 * 60 * 1000,

        }
    )




}




async function send_instruction() {
    return await api.sendMessage(
        instruction,
        {

            model: 'gpt-3.5-turbo',
            // conversationId: conversation_id,
            // temperature: 0.01,
            // maxTokens: 1500,

        }
    )
}


console.log("SENDING INSTRUCTION")

try {

    var resInstruction = await send_instruction();

    conversationId = resInstruction.conversationId;
    parentMessageId = resInstruction.id;

    // log the response
    console.log(resInstruction);

}
catch (e) {

    if (e.statusCode == 400) {
        console.log("ERROR: " + e.message);

        console.log("WAITING 1h");

        // wait 1h
        await new Promise(r => setTimeout(r, 60 * 60 * 1000));

        // try again
        var resInstruction = await send_instruction();

        conversationId = resInstruction.conversationId;
        parentMessageId = resInstruction.id;

    }
    else {
        throw e;
    }
}

console.log("STARTING")


import labels from './data/propaganda/labels.json'  assert { type: 'json' };

// load json from data/propaganda/mult256.json
import data from './data/propaganda/all_data.json'  assert { type: 'json' };

// randomize the order of the data
data.sort(() => Math.random() - 0.5);

let filePath = './data/propaganda/labels.json'

let no_props = ['The text does not contain any identified propaganda techniques', 'No propaganda techniques were identified']

// loop through each example in the data, check if id is in lables
// if it is not, send message with json str of item
//  if it is continue
var counter = 0;

// count data where text is less than 35 and no labels

// let cnt = data.map(item => item.text.length < 35).reduce((a, b) => a + b, 0)

// log how many already done
console.log("ALREADY DONE: " + labels.length + " / " + data.length)

async function start_task() {

    for (const item of data) {

        // item is a json dict with key id, explanation
        // check if id is in labels
        // labels is a list of json dicts with key id, explanation
        // so we need to loop through labels and check if item.id is in labels
        // if it is not, send message with json str of item


        if (!labels.some(label => label.id === item.id)) {

            // if lenght of item.text is less than 35 and no labels, continue
            if (item.text.length < 60 && item.labels.length == 0) {

                // check if
                continue
            }
            if (item.text.length < 70 && item.labels.length > 0) {

                // find the previeous item, add it as context
                // id is formatted is {id}_{paragraph number}
                // so we need to split the id by _ and get the first element and subtract 1

                let prev_id = item.id.split('_')[0]
                let prev_paragraph = parseInt(item.id.split('_')[1]) - 1
                // does this converts to int?
                let prev_full_id = prev_id + '_' + prev_paragraph.toString()

                // find the item with the prev_full_id
                let prev_item = data.find(item => item.id === prev_full_id)

                // if it is null, continue or undefined, continue
                if (prev_item == null || prev_item == undefined) {


                    continue
                }
                // add the prev_item.text as context before it, also add the labels

                item.text = prev_item.text + '\n' + item.text
                item.labels = prev_item.labels.concat(item.labels)
                item.id = prev_full_id + '_' + item.id.split('_')[1]

                // check if this has already been labeled
                if (labels.some(label => label.id === item.id)) {
                    continue
                }

                console.log("Adding context to item", item)

            }
            else if (item.text.length < 70 && item.labels.length > 0) {

                // check if label has Repetition
                if (item.labels.some(label => label === "Repetition")) {
                    // find the previeous item, add it as context
                    // id is formatted is {id}_{paragraph number}
                    // so we need to split the id by _ and get the first element and subtract 1

                    let prev_id = item.id.split('_')[0]
                    let prev_paragraph = parseInt(item.id.split('_')[1]) - 1

                    let prev_full_id = prev_id + '_' + prev_paragraph.toString()

                    // find the item with the prev_full_id
                    let prev_item = data.find(item => item.id === prev_full_id)

                    // if it is null, continue or undefined, continue
                    if (prev_item == null || prev_item == undefined) {
                        continue
                    }
                    // add the prev_item.text as context before it, also add the labels

                    item.text = prev_item.text + '\n' + item.text
                    item.labels = prev_item.labels.concat(item.labels)
                    item.id = prev_full_id + '_' + item.id.split('_')[1]

                    // check if this has already been labeled
                    if (labels.some(label => label.id === item.id)) {
                        continue
                    }

                    console.log("Adding context to item", item)

                }


                // does this converts to int?
            }

            // for item.labels, remove start end keys

            delete item.language

            // send message with JSON string of item
            console.log("Sending message", item);
            let res = await send_message(JSON.stringify(item, null, 4));
            console.log(res);
            // parse the response to see if it's a valid JSON
            try {
                let json = JSON.parse(res.text);
                // if it is, add it to labels, which is a list

                // check if items.lables not empty and "no propaganda "

                // lower case check
                // check if item.labels is not empty and "no propaganda "
                if (item.labels.length > 0) {
                    // check if json.explanation contains no propaganda
                    if (no_props.some(prop => json.explanation.toLowerCase().includes(prop.toLowerCase()))) {
                        {
                            console.log("PANIC, NO PROPAGANDA WAS IDENTIFIED, BUT LABELS WERE PRESENT")
                            continue
                        }
                    }
                }


                labels.push(json);
                // save labels to file

                counter += 1;

                console.log(counter);

                let jsonData = JSON.stringify(labels, null, 4);

                try {
                    await writeFile(filePath, jsonData, 'utf8');
                    console.log('JSON file has been saved successfully.');
                } catch (err) {
                    console.error('An error occurred while saving the JSON file:', err);
                }

            } catch (error) {
                console.log(error);
            }
        } else {
            // if it is, continue
            continue;
        }
    }
}

// try start_task();, when it fails, wait 30 seconds and try again

async function start() {
    try {
        await start_task();
    } catch (error) {
        if (error.statusCode == 400) {

            console.log("Too many requests, waiting 1 hour");
            setTimeout(start, 1 * 60 * 60 * 1000);

        }
        else {
            console.log(error);
            setTimeout(start, 30000);
        }
    }
}

start();