import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses


start_date = "2022-08-01"
end_date = "2023-09-01"
env = yerbamate.Environment()

hashtags = ["ننگ_بر_فتنه_۵۷"]

save_path = os.path.join(
        env["save_path"], "hashtags"
)

files = os.listdir(save_path)
total = 0
for file in files:
    if file.endswith("live.parquet"):
        df = pd.read_parquet(os.path.join(save_path, file))
        # ipdb.set_trace()
        # print(shapre(df)
        # print(file, df.shape[0])
        print(file, df.shape[0])
        total += df.shape[0]

print("Total tweets:", total)
# df = pd.read_parquet(save_path)

# ipdb.set_trace()