import os
import tqdm
import yerbamate
import pandas as pd
import re
import ipdb
import dask.dataframe as dd
import dask.array as da
import vaex

env = yerbamate.Environment()


def combine():

    users = os.listdir(os.path.join(env["data"], "tweets"))
    # tqdm.tqdm(users)
    files = []
    for f in tqdm.tqdm(users):

        file = os.path.join(env["data"], "tweets", f"{f}")

        if os.path.exists(file) and os.stat(file).st_size != 0 and f.endswith(".parquet"):
            files.append(file)

    # save file list that are going to be combined as a dask dataframe list
    # df = pd.DataFrame(files)
    # df.to_csv(os.path.join(env["data"], "tweets", "c_list.csv"), index=False)

    # ipdb.set_trace()

    df: vaex.DataFrame = vaex.open_many(files)
    # ipdb.set_trace()
    print("Shape:", df.shape)
    # ipdb.set_trace()
    df = df.groupby(['text'], agg={'__hidden_count': vaex.agg.count()}).drop('__hidden_count')

    # print size
    print("After:", df.shape)
    
    df.export(os.path.join(env["data"], "all_tweets.parquet"))
    # df.export_hdf5(os.path.join(env["data"], "tweets.hdf5"))

    print("Exported to:", os.path.join(env["data"], "all_tweets.parquet"))
    # df = dd.read_parquet(files, engine='pyarrow')
    # # drop duplicates
    # df = df.drop_duplicates('text')

    # # save
    # df.to_parquet(os.path.join(env["data"], "tweets.parquet"), index=False)

    #


if __name__ == "__main__":
    combine()
