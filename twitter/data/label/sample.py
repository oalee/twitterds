import yerbamate, os, ipdb
import vaex
from ..pre.clean import clean_tweet


env = yerbamate.Environment()


root_data_path = env["save_path"]
sub_path = "time"

# root_path + sub_path contains .parquet files

# load the parquet files

parquet_files = [
    os.path.join(root_data_path, sub_path, file)
    for file in os.listdir(os.path.join(root_data_path, sub_path))
    if file.endswith(".parquet")
]


# load the parquet files into a dask dataframe

df = vaex.open_many(parquet_files)
# sample the dataframe


def sample(df, n=50):
    return df.sample(n)


# ipdb.set_trace()
size = env.get_hparam("sample_size", 5)

# sample the dataframe
sampled_df = sample(df, size)

# get the rawContent column, which contains the tweets, clean and print them
cleanedContent = sampled_df["rawContent"].apply(clean_tweet)

# make json file, input = cleanedContent, output = json file

# save the json file

# save the sampled dataframe
# make a dictionary . map cleaned content to key of "input"
# map the rawContent to the key of "output"
# save the dictionary as a json file
ipdb.set_trace()
my_dict = {"input": item for item in cleanedContent}

print(my_dict)

ipdb.set_trace()
