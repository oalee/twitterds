
import { ChatGPTAPI } from 'chatgpt'

import { ChatGPTUnofficialProxyAPI } from 'chatgpt'

import { writeFile } from 'fs/promises'

import readline from 'readline';

import fs from 'fs';
// const api = new ChatGPTAPI({ apiKey: process.env.OPENAI_API_KEY })

const api = new ChatGPTUnofficialProxyAPI({
    accessToken: process.env.OPENAI_ACCESS_TOKEN_2,
    apiReverseProxyUrl: 'https://api.pawan.krd/backend-api/conversation'
})


let revaluatePrompt = `
Please conduct a thorough review of your previous output. Consider the following key points in your assessment:

- Is there any bias or propaganda present?
- Is there any bias in your analysis?
- Is the negative sentiment towards the Islamic Republic?
- Is there a negative or positive connotations for a (political) ideology, such as right-wing, left-wing, nationalism or religious? Is this included in the explanation and positive or negative target? 
- Does the content reflect everyday life?
- Who are the subjects of negative and positive sentiment?
- Are these sentiments directed towards individuals or groups?
- Is the objective clearly defined?
- Is the explanation in the json i.e., reasoning behind the identified positive and negative targets and the techniques are clear, comprehensive and presented in the explanation? If not, please elaborate and include them in the explanation.
- Is there the identified positive target clearly explained in the explanation? Please do not include any positive target that is loosely inferred or not clearly explained in the explanation.
- Does the text contains positive connotation toward Women.Life.Freedom movement? if so, is it included in the positive target and explained in the explanation. For example, calls for justice, and indications of peoples's freedom of choice, such as the right to choose their own clothing, dancing and pro LGBTQ statements are positive connotations among others.

After your evaluation, please generate a revised JSON output if there are any errors or inaccuracies in your previous response. Strive for impartiality and avoid any biases inherent in your training data. 
IMPORTANT, your output should strictly be in JSON format without any additional text for the origianl task; make sure to NOT mention this prompt, further analysis or the previous outputs, or how you changed your analysis in your explanation. Your explanation should be comprehensive for the original task.
`

var instruction = `
Role and Instruction: Analysis Propaganda Techniques from Tweets around the Women.Life.Freedom Movement

Context:
The women.life.freedom movement was triggered by a distressing incident involving Mahsa Amini, a 22-year-old Iranian Kurdish woman who fell prey to Iran's "morality" police. This event instigated wide-ranging protests across Iran, which were met with a violent reprisal from the regime resulting in the horrifying death of over 500 people, including a significant number of school children, across various cities. Since the death of Mahsa Amini, a movement with the main slogan of Women Life Freedom began in Iran, with the aim of freeing women and their life from oppressive Islamic rules and participatory.
There has been a record of serial chemical attacks on school girls happening for more than three months, meanwhile the Islamic republic has not found the culprit, many experts say the culprit is Islamic republic and the lack of investigations and culprit founds indicates the fact this is orchestrated by Islamic republic's terrorist forces. Student are not safe in schools and have been subjected to oppression.
This reaction led to the Islamic Republic being categorized as a terrorist entity. Public sentiment holds intense feelings of resentment and antagonism towards the Islamic Republic of Iran, largely due to its enforcement of what are seen as oppressive Islamic values. 
Since the death of Mahsa Amini, a movement characterized by "Women.Life.Freedom" began in Iran to overthrow the dictatorship in Iran, i.e., the Islamic republic and the head of state, Khamenei. However, this movement is not nationalistic in nature, as the main goal, is beyond borders, for example, the women in Afganistan were the first ones in the world to show solidarity with women in Iran after death of Mahsa Amini.
The Women.Life.Freedom movement is a social movement focused on advocating for women's rights, equality, and freedom. It emerged as a response to various forms of oppression and discrimination faced by women in different societies worldwide. The movement aims to challenge patriarchal norms, fight against gender-based violence, and strive for equal opportunities and choices for women in all aspects of life.

Women.Life.Freedom recognizes that women's experiences and struggles are diverse, shaped by factors such as culture, religion, socio-economic status, and political systems. It seeks to create a platform where women can share their stories, amplify their voices, and mobilize collective action towards achieving gender equality and dismantling systemic barriers.

On social media platforms, offensive language is often tweaked to circumvent content detection systems. For instance, "Qنی", equivalent to "کونی" in Persian, is a derogatory term with negative connotations towards homosexuality. Terms such as "اسی" is derogatorily used for Hamed Esmailioun. Moreover "مسی" and "عنینژاد", "قمیلکا" is derogatory language used to mock "مسیح علینژاد" Masih Alinejad. Hamed Esmalition and Masih Alinejad and Reza Pahlavi are known opposition figures of Islamic Republic.

Task Description: Your task is to analyze tweets to uncover propaganda around women.life.freedom movement. Your goal is to discover the techniques used around the movement, pinpoint the positive and negative targets of the tweet (the entities the tweet aims to support or undermine), and define the objectives of the tweet. 

Instructions:


Tweet Analysis: Begin by thoroughly studying the tweet, translating to English and the prevailing public sentiment, and any potential concealed language or euphemisms. If the tweet exhibits hate, or loaded language toward the Islamic republic or its authorities, ensure that the Islamic Republic is identified as a negative target. Islamic Republic is enemy of women.life.freedom, and negative sentiment toward Islamic republic is not against women.life.freedom.

Technique Detection and Explanation: Then, your next step should be to recognize the techniques deployed in the tweet. Keep in mind the cultural and sociopolitical context of Iran. Upon identifying the techniques, delve into a detailed explanation of how each technique is employed, complete with examples. The explanation should be comprehensive and should not be limited to a single sentence. If there is no propaganda in the text, the explanation should start with "No propaganda is detected in this tweet." and should be followed by a comprehensive explanation of why there is no propaganda in the tweet.

Identify the Targets: Your third task is to identify the positive_target(s) and negative_target(s). These are the entities or ideologies that the tweet is attempting to support or undermine, respectively. Make sure to elucidate and include explanations of the reasoning behind the labeled targets in the explanation section. Make sure to identify positive or negative connotations towards (political) ideologies, such as nationalism, left wing, right wing, religios, etc. If the tweet exhibits hate or criticism towards the Islamic Republic, make sure to include the Islamic Republic as a negative target. 

Uncover the Objectives: Lastly, elucidate the objectives of the tweet. These objectives could be the desired reactions, feelings, or actions that the tweeter wants to incite in the audience.

Technique Labeling: During your analysis, assign suitable labels to the identified techniques from the following list:
Appeal_to_Authority
Appeal_to_Popularity
Appeal_to_Values
Appeal_to_Fear-Prejudice
Flag_Waving
Causal_Oversimplification
False_Dilemma-No_Choice
Consequential_Oversimplification
Straw_Man
Red_Herring
Whataboutism
Slogans
Appeal_to_Time
Conversation_Killer
Loaded_Language
Repetition
Exaggeration-Minimisation
Obfuscation-Vagueness-Confusion
Name_Calling-Labeling
Doubt
Guilt_by_Association
Appeal_to_Hypocrisy
Questioning_the_Reputation


Input:

json

{
"id": "<tweet_id>",
"text": "<tweet_text>"
}

Output:

json

{
"id": "<tweet_id>",
"eng":"<english_translation>",
"explanation": "<comprehensive_explanation_of_techniques_with_examples>",
"labels": ["<technique>", "..."],
"positive_target": ["<positive_target>", "..."],
"negative_target": ["<negative_target>", "..."],
"objectives": ["<objective>", "..."]
}

IMPORTANT: Only use the labels listed above for your analysis
IMPORTANT: Your explanation must be comprehensive explanation and exemplifying the techniques and their usage. The output should strictly be in JSON format, with no preceding text.
IMPORTANT: Make sure to put the Islamic Republic as the negative target in case of clerics, authorities, etc. The Islamic Republic is the negative target in case of hate or criticism towards the Islamic Republic or any positive emotion toward women.life.freedom as this is a revolution.
IMPORTANT: IRGC and Islamic republic are terrorist because of their crimes and actions in Iran
IMPORTANT: Negative connotations towards IRGC as terrorist and Islamic Republic should count as raising awareness for their crimes in Iran.
IMPORTANT: Don't forget to include "women.life.freedom" as positive target of tweets that want to help this movement move forward.
IMPORTANT: Khamenei, the supreme leader of Iran has killed many indirectly and is a dictator. Count him as one.  Islamic republic's authorities, administration, IRGC do count as Islamic republic.
IMPORTANT: Consider that the text could be emotional response, not from propagandaist.
IMPORTANT: Islamic Republic, its authorities and Khamenei are opposed to women.life.freedom
IMPORTANT: Women.Life.Freedom started as a movement and a revolution to overthrow the dictator of Iran and Islamic Republic with the aim of reaching to "Freedom for Women's Life"
IMPORTANT: Opressive forces of Islamic republic are Islamic republic and oppressive forces against women.life.freedom refer to them.
IMPORTANT: Make sure to explain the reason why you chose both negative and positive targets and include it in the explanation.
IMPORTANT: Using hashtags such as Mahsa Amini does NOT always implies positivity toward women.life.freedom, it could be hijacked by propagandaists to promote specific political idelogy or motive.

Only reply by "..." if you understand the task and are ready to begin.
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

async function sendQuery(message, parent) {
    return await api.sendMessage(
        message,
        {
            parentMessageId: parent.id,
            model: 'gpt-3.5-turbo',
            conversationId: parent.conversationId,
            timeoutMs: 2 * 60 * 1000,
            temperature: 0.0001,

        }
    )
}




async function send_instruction() {
    return await api.sendMessage(
        instruction,
        {

            model: 'gpt-3.5-turbo',
            // conversationId: conversation_id,
            // temperature: 0.00,
            // maxTokens: 1500,

        }
    )
}

console.log("STARTING")


import labels from './data/propaganda/pers_labels.json'  assert { type: 'json' };

// load json from data/propaganda/mult256.json
import data from './data/propaganda/sample.json'  assert { type: 'json' };

// randomize the order of the data
// data.sort(() => Math.random() - 0.5);

let filePath = './data/propaganda/pers_labels.json'

let historyPath = './data/propaganda/pers_labels_history.json'

// if file does not exist, create it
if (!fs.existsSync(historyPath)) {

    // create json empty file
    fs.writeFileSync(historyPath, JSON.stringify({}));
}

// read history file
let history = fs.readFileSync(historyPath, 'utf8');
// parse history file
history = JSON.parse(history);


let no_props = ['The text does not contain any identified propaganda techniques', 'No propaganda techniques were identified']

// loop through each example in the data, check if id is in lables
// if it is not, send message with json str of item
//  if it is continue
var counter = 0;

// count data where text is less than 35 and no labels

// let cnt = data.map(item => item.text.length < 35).reduce((a, b) => a + b, 0)

// log how many already done
console.log("ALREADY DONE: " + labels.length + " / " + data.length)

// sends instruction, saves history to item.id key, history is array of responses
async function do_one_item(item) {

    // check if history has item.id key
    if (history[item.id]) {
        console.log("SKIPPING: " + item.id);
        return null;
    }

    // if media is true and text is less than 35, skip
    if (item.media && item.rawContent.length < 55) {
        console.log("SKIPPING: " + item.id);
        return null;
    }

    let instructionUqn = await send_instruction();

    console.log("SENT INSTRUCTION: " , instructionUqn);


    let hist = [{ "role": "user", "data": instruction }, instructionUqn];

    var sendingItem = { id: item.id, text: item.rawContent };
    sendingItem = JSON.stringify(sendingItem);

    var saveCnt = 0

    while (true) {
        //   let jsonString = JSON.stringify(sendingItem);

        console.log("Sending: ", sendingItem);
        // add sendingItem to history with role of user, add "role": "user" to sendingItem 

        // find the last item in hist with role "assistant"

        let lastAssistant = hist.filter(item => item.role == "assistant")   
        lastAssistant = lastAssistant[lastAssistant.length - 1]

        let res = await sendQuery(sendingItem, lastAssistant);

        hist.push({
            role: "user",
            data: sendingItem
        });

        hist.push(res);
        console.log(res);

        //   log if res.text is not json

        try {

            // replace '`' with '''
            res.text = res.text.replace(/`/g, "'");
            let output = JSON.parse(res.text);
            res.parsedOutput = output;

            saveCnt += 1;

            // add to history
            history[item.id] = {
                history: hist,
                item: item
            }


            if (saveCnt > 1) {
                return res;

            }

            sendingItem = revaluatePrompt
            continue;


            return res;
        }
        catch (e) {
            console.error(e);
            sendingItem = "Not a valid json, only output json. IMPORTANT only output json with no other text";
        }
    }
}



async function start_task() {

    for (const item of data) {


        let jsonz = await do_one_item(item);

        if (jsonz === null) {
            continue;
        } else {
            // log the response
            console.log("response", jsonz);
        }
        let rsponse = JSON.parse(jsonz.text);
        rsponse['text'] = item['rawContent'];
        labels.push(rsponse);
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

        // also save history
        let historyData = JSON.stringify(history, null, 4);

        try {
            await writeFile(historyPath, historyData, 'utf8');
            console.log('JSON file has been saved successfully.');
        }
        catch (err) {
            console.error('An error occurred while saving the JSON file:', err);
        }


    }


}


async function getUserInput() {

    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    return new Promise((resolve) => {
        rl.question('', (answer) => {
            rl.close();
            resolve(answer);
        });
    });
}
// try start_task();, when it fails, wait 30 seconds and try again

async function start() {
    try {
        await start_task();
    } catch (error) {
        if (error.statusCode == 400) {

            console.log("Too many requests, waiting 3 hour");
            setTimeout(start, 1 * 60 * 60 * 1000);

        }
        else {
            console.log(error);
            setTimeout(start, 30000);
        }
    }
}

start();