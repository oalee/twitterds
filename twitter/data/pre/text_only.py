import os
import pandas as pd
import yerbamate


env = yerbamate.Environment()


# data_path = env['data']

users_path = env['users']


# folder, each has a tweets.parquet file, need to read it and get the rawContent column


def main():

    users = os.listdir(users_path)

    for user in users:

        # read the parquet file
        user_path = os.path.join(users_path, user)
        tweets_path = os.path.join(user_path, 'tweets.parquet')

        try:
            df = pd.read_parquet(tweets_path)
        except Exception as e:
            print(f"Error reading {tweets_path}: {e}")
            continue

        # get the rawContent column
        raw_content = df['rawContent']

        # write it to a file, tweets.txt
        tweets_txt_path = os.path.join(user_path, 'tweets.txt')

        with open(tweets_txt_path, 'w') as f:
            for tweet in raw_content:
                f.write(f"{tweet}\n")


if __name__ == '__main__':
    main()
