import os
import yerbamate
import pandas as pd
import ipdb

env = yerbamate.Environment()


hashtags = ["ننگ_بر_فتنه_۵۷", "مرگ_بر_سه_فاسد_ملا_چپی_مجاهد" , "مرد_میهن_آبادی", "زن_زندگی_آزادی"]


wlf = hashtags[-1]


hashtag = wlf

startdate="2022-08-01"
enddate="2023-09-01"



save_path = os.path.join(
    env["save_path"], "query", "زن_زندگی_آزادی_2022-08-01_2022-09-01_live.parquet"
)

df = pd.read_parquet(save_path)

# save csv

df.to_csv(os.path.join(env["save_path"], "query", f"{hashtag}_{startdate}_{enddate}_live.csv"))

# print (df)

# ipdb.set_trace()
# 
# do the same thing for _top 

save_path = os.path.join(
    env["save_path"], "query", "زن_زندگی_آزادی_2022-08-01_2022-09-01_top.parquet"
)

df = pd.read_parquet(save_path)

# save csv

df.to_csv(os.path.join(env["save_path"], "query", f"{hashtag}_{startdate}_{enddate}_top.csv"))