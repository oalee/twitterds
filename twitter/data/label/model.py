import requests, random, time, ipdb

chars = "abcdefghijklmnopqrstuvwxyz1234567890"


class Model:
    session_id = "".join([random.choice(chars) for i in range(16)])
    user_fake_id = "".join([random.choice(chars) for i in range(16)])
    chat_id = "0"

    def send_prompt(self, prompt):
        # revalidate session_id and user_fake_id

        self.session_id = "".join([random.choice(chars) for i in range(16)])
        self.user_fake_id = "".join([random.choice(chars) for i in range(16)])
        return self.GetAnswer(prompt)

    def start_new_chat(self):
        self.chat_id = "0"
        self.session_id = "".join([random.choice(chars) for i in range(16)])
        self.user_fake_id = "".join([random.choice(chars) for i in range(16)])

    def GetAnswer(self, prompt: str):
        r = requests.post(
            "https://chatgptproxy.me/api/v1/chat/conversation",
            json={
                "data": {
                    "parent_id": self.chat_id,
                    "session_id": self.session_id,
                    "question": prompt,
                    "user_fake_id": self.user_fake_id,
                }
            },
        ).json()

        if r["code"] == 200 and r["code_msg"] == "Success":
            self.chat_id = r["resp_data"]["chat_id"]
            r = requests.post(
                "https://chatgptproxy.me/api/v1/chat/result",
                json={
                    "data": {
                        "chat_id": self.chat_id,
                        "session_id": self.session_id,
                        "user_fake_id": self.user_fake_id,
                    }
                },
            ).json()
            if r["code"] == 200 and r["code_msg"] == "Success":
                if r["resp_data"]["answer"] != "":
                    return r["resp_data"]["answer"]
                while r["resp_data"]["answer"] == "":
                    time.sleep(1)
                    r = requests.post(
                        "https://chatgptproxy.me/api/v1/chat/result",
                        json={
                            "data": {
                                "chat_id": self.chat_id,
                                "session_id": self.session_id,
                                "user_fake_id": self.user_fake_id,
                            }
                        },
                    ).json()
                    if (
                        r["code"] == 200
                        and r["code_msg"] == "Success"
                        and r["resp_data"]["answer"] != ""
                    ):
                        return r["resp_data"]["answer"]
            else:
                if "operation too frequent" in r["code_msg"].lower():
                    print("Operation too frequent for result. Waiting 10 seconds...")
                    time.sleep(10)
                    return self.GetAnswer(prompt)
                print(
                    f"There was an error with your request for result. The response was: {r}"
                )
                return
        else:
            if "operation too frequent" in r["code_msg"].lower():
                print("Operation too frequent for question. Waiting 10 seconds...")
                time.sleep(10)
                return self.GetAnswer(prompt)
            elif "Your question has been received" in r["code_msg"]:
                print(
                    "Session id or user fake id probably already in use. Generating new ones..."
                )
                self.session_id = "".join([random.choice(chars) for i in range(32)])
                self.user_fake_id = "".join([random.choice(chars) for i in range(16)])
                return self.GetAnswer(prompt)
            print(
                f"There was an error with your request for question. The response was: {r}"
            )
            return


if __name__ == "__main__":
    model = Model()

    # prompt =
    prompt = """
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

Text sample: '''این جوان لیبیایی عکس هایی که در سال ۲۰۰۰ در شهر بنغازی لیبی گرفته بود را مجددا در همان موقعیت در سال ۲۰۱۸ گرفته است. نتیجه براندازی سفارشی اروپایی-آمریکایی با مشارکت کمپانی های نفتی، تبدیل شدن به زمین سوخته. https://t.co/J4Zk4Mdcl8'''


Entity: 'Hamed Esmailion'

Your JSON response:
    """
    print("Bot:", model.GetAnswer(prompt))

    while True:
        prompt = input("You: ")
        print("Bot:", model.GetAnswer(prompt))
