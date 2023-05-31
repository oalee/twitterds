# find top liked tweets


import os
import sys
import yerbamate

import vaex
import ipdb
import dask.dataframe as dd

env = yerbamate.Environment()

time_path = os.path.join(env["data"], "time")


def main():
    # parquet files, formatted as "YYYY-MM_tweets.parquet"
    # we have 2022-08 to 2023-04
    # we want to find the top tweets for each month

    # for each month, find the top tweets

    parquet_files = os.listdir(time_path)
    parquet_files = [f for f in parquet_files if f.endswith(".parquet")]

    for parquet_file in parquet_files:
        print(parquet_file)
        df = dd.read_parquet(os.path.join(time_path, parquet_file))
        # df = vaex.open(os.path.join(time_path, parquet_file))
        # apply int to likeCount
        # df["likeCount"] = df["likeCount"].astype(int)
        # ipdb.set_trace()
        df["likeCount"] = df["likeCount"].astype(int)
        df = df[df["likeCount"] > 1000]
        # df = df.sort(by="likeCount", ascending=False)
        # compute
        # pnds = df.to_pandas_df()
        # df = df[:100]

        # print size of df
        print(df.shape)
        # save as top_tweets.parquet in time_path and in month_path
        path = os.path.join(time_path, "top", parquet_file)
        # makedirs if not exist
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # 

        # only save id, rawContent, likeCount
        # df = df[["id", "rawContent", "likeCount"]]

        df.to_parquet(path, engine="pyarrow", overwrite=True)


if __name__ == "__main__":
    main()
