"""
Originally written by Ali Hurriyetoglu (github.com/ahurriyetoglu)
Edited by Osman Mutlu (github.com/OsmanMutlu)
"""

import pymongo
import json
import re
from unicode_tr import unicode_tr

repo_path = "/home/username"

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
user_col = db["users"]

# Collect the previously created location dictionary
# This dictionary was created using 'https://tr.wikipedia.org/wiki/T%C3%BCrkiye%27nin_illeri' and
# 'https://tr.wikipedia.org/wiki/T%C3%BCrkiye%27nin_il%C3%A7eleri'
with open(repo_path + "/twitter_database/user_analysis/tr_provinces20211120.json", "r", encoding="utf-8") as f:
    province_dicts = json.load(f)

# Create the regexes
province_rgxes = {}
all_provinces = []
for province in province_dicts:
    province_pttrn = r'(?P<_'+str(province['province_code'])+r'>\b' + \
                     r'([dtl][aeiıuü](n|y[iıuü]m)?)?\b|\b'.join(province['province_name_variants']) + \
                     r'([dtl][aeiıuü](n|y[iıuü]m)?)?\b)'
    province_rgx = re.compile(province_pttrn, re.I)
    province_rgxes[province['province_code']] = province_rgx
    all_provinces += province['province_name_variants']

all_provinces_rgx = re.compile(r'\b' + \
                               r'([dtl][aeiıuü](n|y[iıuü]m)?)?\b|\b'.join(sorted(all_provinces, key=len, reverse=True)) + \
                               r'([dtl][aeiıuü](n|y[iıuü]m)?)?\b', re.I)

# Process users
users = user_col.find({"province_codes": None}, ["location", "description", "screen_name"])
total_processed = 0
for user in users:
    id_str = user['_id']
    # Proper unicode for the user's info
    location = unicode_tr(user.get('location', '').strip()).lower()
    description = unicode_tr(user.get('description', '').strip()).lower()
    if (len(location + description) < 5):
        user_col.update_one({"_id": id_str}, {"$set": {"province_codes": []}})
        continue
    screen_name = unicode_tr(user['screen_name'].strip()).lower()
    total_processed += 1

    new_province_codes = []
    # search with regex in location
    if all_provinces_rgx.search(location):
        for pcode, rx in province_rgxes.items():
            for m in rx.finditer(location):
                new_province_codes.append({'source':'location', 'pcode':pcode})

    # search with regex in description
    if all_provinces_rgx.search(description):
        for pcode, rx in province_rgxes.items():
            for m in rx.finditer(description):
                new_province_codes.append({'source':'description', 'pcode':pcode})

    # search with regex in screen_name
    if all_provinces_rgx.search(screen_name):
        for pcode, rx in province_rgxes.items():
            for m in rx.finditer(screen_name):
                new_province_codes.append({'source':'screen_name', 'pcode':pcode})

    # Update the user
    user_col.update_one({"_id": id_str}, {"$set": {"province_codes": new_province_codes}})

print("Processed {} users in total.".format(str(total_processed)))
