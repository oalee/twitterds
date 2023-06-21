import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd


time_path = os.path.join(env["data"], "time")

new = os.path.join(env["data"], "new")

parquet_files = os.listdir(time_path)
parquet_files = [f for f in parquet_files if f.endswith(".parquet")]
parquet_files = [os.path.join(time_path, f) for f in parquet_files]


# ipdb.set_trace()


# use dask, to convert date to pandas datetime

for parquet_file in parquet_files:
    print(parquet_file)
    df = dd.read_parquet(parquet_file)
    df["date"] = dd.to_datetime(df["date"])

    nwe_parquet_file = parquet_file.replace(time_path, new)
    # make sure the new directory exists
    os.makedirs(os.path.dirname(nwe_parquet_file), exist_ok=True)
    # replace the old parquet file
    df.to_parquet(nwe_parquet_file, engine="pyarrow")
