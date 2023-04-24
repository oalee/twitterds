import pandas as pd

import yerbamate
import ipdb
import os, tqdm

env = yerbamate.Environment()


path = os.path.join(env["data"], "tweets")

dirs = os.listdir(path)

for d in tqdm.tqdm(dirs):
    # check if empty
    # first check if size is less than 10kb
    if os.stat(os.path.join(path, d)).st_size < 10000:

        df = pd.read_parquet(os.path.join(path, d))
        if df.size == 0:
            # ipdb.set_trace()
            os.remove(os.path.join(path, d))
            print("Removed:", os.path.join(path, d))
