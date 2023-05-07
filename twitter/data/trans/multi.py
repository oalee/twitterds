import os
import sys
import json
import time
import tqdm
import yerbamate
import multiprocessing as mp
from googletrans import Translator

env = yerbamate.Environment()

translator = Translator()

def translate(text):
    if not text or text == '':
        return ''

    try:
        return translator.translate(text, src='en', dest='fa').text
    except Exception as e:
        print(f"Error translating text: {e}")
        return text

def translate_item(item):
    return {
        'instruction': translate(item['instruction']),
        'input': translate(item['input']),
        'output': translate(item['output'])
    }

def load_translated_data(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return []

def save_translated_data(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def main():
    alpaca = env['alpaca']

    with open(alpaca, 'r') as f:
        data = json.load(f)

    alpaca_translated = env['alpaca_translated']
    translated = load_translated_data(alpaca_translated)
    translated_set = {json.dumps(item, sort_keys=True) for item in translated}
    remaining_data = [item for item in data if json.dumps(item, sort_keys=True) not in translated_set]

    # with mp.Pool() as pool:
    #     new_translated = list(tqdm.tqdm(pool.imap(translate_item, remaining_data), total=len(remaining_data)))
    save_interval = 100

    with mp.Pool() as pool:
        for i in tqdm.tqdm(range(0, len(remaining_data), save_interval)):
            batch = remaining_data[i:i + save_interval]
            new_translated = list(pool.imap(translate_item, batch))

            translated.extend(new_translated)
            save_translated_data(alpaca_translated, translated)


    translated.extend(new_translated)
    save_translated_data(alpaca_translated, translated)

if __name__ == '__main__':
    main()
