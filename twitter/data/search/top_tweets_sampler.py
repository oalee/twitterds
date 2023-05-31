import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()

time_path = os.path.join(env["data"], "time", "top")


def read_all():
    parquet_files = os.listdir(time_path)
    parquet_files = [
        f
        for f in parquet_files
        if f.endswith(".parquet")
        and not f.startswith("all")
        and not f.startswith("sample")
    ]
    dfs = []
    for parquet_file in parquet_files:
        print(parquet_file)
        df = dd.read_parquet(os.path.join(time_path, parquet_file))
        dfs.append(df)
    df = dd.concat(dfs)
    return df


def merge():
    df = read_all()
    df = df.compute()
    df.to_parquet(os.path.join(time_path, "all.parquet"))


def read_merged():
    df = pd.read_parquet(os.path.join(time_path, "all.parquet"))
    return df


def sample(df, n=1000):
    df = df.sample(n)
    df.to_parquet(os.path.join(time_path, "sample.parquet"))
    return df


if env.action == "merge":
    merge()
elif env.action == "json":
    df = read_merged()
    df = sample(df)
    df = df.reset_index()
    # ipdb.set_trace()
    df = df.to_json(os.path.join(time_path, "sample_2.json"), orient="records", force_ascii=False)
    # print(df)

else:
    df = read_merged()
    sample(df)
