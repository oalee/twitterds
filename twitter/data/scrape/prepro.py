import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses


start_date = "2022-08-01"
end_date = "2023-09-01"
env = yerbamate.Environment()

hashtags = ["ننگ_بر_فتنه_۵۷"]

save_path = os.path.join(
        env["save_path"], "hashtags", f"{hashtags[0]}_{start_date}_{end_date}.parquet"
)

df = pd.read_parquet(save_path)

ipdb.set_trace()