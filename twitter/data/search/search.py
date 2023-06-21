import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd


time_path = os.path.join(env["data"], "time")


def search(df, search_term):
    # ipdb.set_trace()
    # drop na
    df = df.dropna(subset=["rawContent"])
    df = df[df["rawContent"].str.contains(search_term)]
    # df.to_parquet(os.path.join(time_path, "search.parquet"))
    return df


def search_all(search_term):

    parquet_files = os.listdir(time_path)
    parquet_files = [f for f in parquet_files if f.endswith(".parquet")]

    dfs = []
    for parquet_file in parquet_files:
        print(parquet_file)
        df = dd.read_parquet(os.path.join(time_path, parquet_file))
        # ipdb.set_trace()
        df = search(df, search_term)

        dfs.append(df.compute())

    df = dd.concat(dfs)

    return df

search_term = env.action

df = search_all(search_term)

ipdb.set_trace()