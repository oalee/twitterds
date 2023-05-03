
# converst a dataframe to a txt file

import os
import vaex

from yerbamate import Environment


env = Environment()


# first open the dataframe with vaex

def df_to_txt(src, dst):
    if dst is None:
        # replace whatever is after the last dot with .txt
        dst = src[:src.rfind(".")] + ".csv"
    df = vaex.open(src)

    # save the text column to a txt file
    df.export(dst, progress=True)


if __name__ == "__main__":
    src = env.args[0]
    dst = env.args[1] if len(env.args) > 1 else None

    df_to_txt(src, dst)
