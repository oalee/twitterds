

# translate alpaca from english to persian

import os
import sys
import json
import time
import tqdm
import yerbamate

from googletrans import Translator


env = yerbamate.Environment()

translator = Translator()

import ipdb

def translate(text):

    # if text is empty, return empty
    if not text or text == '':
        return ''
    ipdb.set_trace()

    try:
        translator.translate(text, src='en', dest='fa').text
    except Exception as e:
        print(f"Error translating text: {e}")
        return text


def main():
    alpaca = env['alpaca']
    # path to json, read it

    with open(alpaca, 'r') as f:
        data = json.load(f)

    translated = []
    for item in tqdm.tqdm(data):
        translated.append({
            'instruction': translate(item['instruction']),
            'input': translate(item['input']),
            'output': translate(item['output'])
        })

    alpaca_translated = env['alpaca_translated']
    # write it back
    with open(alpaca_translated, 'w') as f:
        json.dump(translated, f,  indent=4)


if __name__ == '__main__':
    main()
