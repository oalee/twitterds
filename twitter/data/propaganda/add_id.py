import json
import os
import re
import yerbamate
import ipdb

env = yerbamate.Environment()


# we should have two propaganda paths, arabic, multi language

id = env.get_hparam("id")
p = os.path.join(env["data"], "propaganda", f"multi.json")


data = json.load(open(p))


# the structure is list of dictionaries, each has text and labels
# add ids to each item, and save it as a json file

for i, item in enumerate(data):
    item["id"] = i


json.dump(data, open(p, "w"), indent=4, ensure_ascii=False)
