import yerbamate, os, sys, vaex, ipdb, dask.dataframe as dd, pandas as pd

env = yerbamate.Environment()
import dask.dataframe as dd

# import sqlalchemy

time_path = os.path.join(env["data"], "time")

# engine = sqlalchemy.create_engine('sqlite:////run/media/al/data/td', echo=True)

def convert_all():
    parquet_files = os.listdir(time_path)
    parquet_files = [f for f in parquet_files if f.endswith(".parquet")]

    for parquet_file in parquet_files:
        print(parquet_file)
        df = dd.read_parquet(os.path.join(time_path, parquet_file))

        df.to_sql(
            name="tweets",
            uri = 'sqlite:////run/media/al/data/td/tweets.db',
            if_exists="append",
        )


convert_all()
