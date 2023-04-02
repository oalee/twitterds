import pandas as pd
import snscrape.modules.twitter as twitter
import ipdb
import yerbamate, os, dataclasses, tqdm


start_date = "2022-08-01"
end_date = "2023-09-01"
env = yerbamate.Environment()

hashtags = ["ننگ_بر_فتنه_۵۷"]

save_path = os.path.join(env["save_path"], "users")

files = os.listdir(save_path)
total = 0
cnt = 0
for dir in tqdm.tqdm(files):

    #    os.listdir(os.path.join(save_path, dir))
    # tqdm.tqdm(os.listdir(os.path.join(save_path, dir)))
    for file in os.listdir(os.path.join(save_path, dir)):
        if file.endswith(".parquet"):
            try:
                df = pd.read_parquet(os.path.join(save_path, dir, file))
                # ipdb.set_trace()
                # print(shapre(df)
                # print(file, df.shape[0])
                # ipdb.set_trace()
                # print(file, df.shape[0])
                cnt += 1
                total += df.shape[0]
                # print("-----------------")
            except:
                pass


print("Total tweets:", total, "Total users:", cnt)
# df = pd.read_parquet(save_path)

# ipdb.set_trace()
