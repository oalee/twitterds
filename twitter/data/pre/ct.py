import os
import tqdm
import yerbamate
import pandas as pd
import re
import ipdb
import dask.dataframe as dd
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

    df = dd.read_parquet(files[:5000])
    # ipdb.set_trace()
    print("Shape:", df.size.compute())
    # ipdb.set_trace()
    # df = df.groupby(['text'], agg={'__hidden_count': vaex.agg.count()}).drop('__hidden_count')

    # print size
    # print("After:", df.shape)
    
    # df.export_hdf5(os.path.join(env["data"], "tweets.hdf5"))

    # print("Exported to:", os.path.join(env["data"], "tweets.hdf5"))
    # df = dd.read_parquet(files, engine='pyarrow')
    # # drop duplicates
    df = df.drop_duplicates('text').compute()

    print("Shape:", df.size)


    ipdb.set_trace()

    # # save
    # df.to_parquet(os.path.join(env["data"], "tweets.parquet"), index=False)

    #


if __name__ == "__main__":
    combine()
