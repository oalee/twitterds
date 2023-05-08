import os
import pandas as pd
import tqdm
import yerbamate
from concurrent.futures import ProcessPoolExecutor

env = yerbamate.Environment()

tweets_path = env['tweets']
export_path = env['export']

if not os.path.exists(export_path):
    os.makedirs(export_path)

def pd_to_text(file_path):
    df = pd.read_parquet(file_path)
    texts = df['text']

    tweets_txt_path = os.path.join(
        export_path, file_path.split('/')[-1].split('.')[0] + '.txt')

    with open(tweets_txt_path, 'w') as f:
        for tweet in texts:
            f.write(f"{tweet}\n")

def main():
    files = os.listdir(tweets_path)
    already_done = os.listdir(export_path)
    files = [file for file in files if file not in already_done]

    with ProcessPoolExecutor() as executor:
        file_paths = [os.path.join(tweets_path, file) for file in files]
        list(tqdm.tqdm(executor.map(pd_to_text, file_paths), total=len(files)))

if __name__ == '__main__':
    main()
