"""
Originally written by Ali Hurriyetoglu (github.com/ahurriyetoglu)
Edited by Osman Mutlu (github.com/OsmanMutlu)
"""

import pymongo
import json
import re
from unicode_tr import unicode_tr
from unicode_tr.extras import slugify

repo_path = "/home/username"

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
user_col = db["users"]

# Collect the previously created gender dictionaries
with open(repo_path + "/twitter_database/user_analysis/tr_names20211120.json", "r", encoding="utf-8") as f:
    gender_dicts = json.load(f)

ignore_name_tokens = set(gender_dicts["unisex"] + gender_dicts["irrelevant"] + gender_dicts["irrelevant_proper_name"])
male_names = gender_dicts["first_names"]['male']
male_slugified_names = [slugify(name) for name in male_names]
all_male_names = list(set(male_names + male_slugified_names))
female_names = gender_dicts["first_names"]['female']
female_slugified_names = [slugify(name) for name in female_names]
all_female_names = list(set(female_names + female_slugified_names))
all_names = list(set(all_male_names + all_female_names))

# Regexes
all_names_rgx = re.compile(r'|'.join(sorted(all_names, key=len, reverse=True)), re.I)
male_names_rgx = re.compile(r'|'.join(sorted(all_male_names, key=len, reverse=True)), re.I)
female_names_rgx = re.compile(r'|'.join(sorted(all_female_names, key=len, reverse=True)), re.I)

screen_name_rgx = re.compile(r'|'.join(sorted([nm for nm in all_names if len(nm)>4], key=len, reverse=True)), re.I)

description_male_rgx = re.compile(r"\b(baba(s[iı])?y[ıi]m|dedes[iı]y[iı]m|bir erke[gğk][ıi]m|bir\s+adam[iı]m|o[ğg]luyum|kocas[iı]y[iı]m|k[iı]l[iı]b[iı]k|yak[iı][sş][iı]kl[iı]kelim\b|kel\s+bir|kasl[ıi]\s+bir|kasl[ıi]y[ıi]m|bir\s+bey|beyefendi)", re.I)
description_female_rgx = re.compile(r"\b(anne(s[iı])?y[iı]m|han[iı]m[iı]y[iı]m|bir\s+kad[iı]n[iı]m|k[iı]z[iı]y[iı]m|anneannes[iı]y[iı]m|do[ğg]urdum|do[ğg]um\s+yap|sa[cç][ıi]\s+uzun|ev\s+k[ıi]z[ıi]|oje|rimel|makyaj)", re.I)


# Process users
users = user_col.find({"genders": None}, ["description", "name", "screen_name"])
total_processed = 0
for user in users:
    # Proper unicode for the user's info
    description = unicode_tr(user.get('description', '').strip()).lower()
    name = unicode_tr(user['name'].strip()).lower()
    screen_name = unicode_tr(user['screen_name'].strip()).lower()
    id_str = user['_id']
    total_processed += 1
    new_genders = []

    # Regex match name
    tokens = re.findall(r'\b\w+', name, re.I)
    if len(tokens) > 0 and tokens[0] not in ignore_name_tokens:
        first_token = tokens[0] # first name

        name_match = all_names_rgx.match(first_token)
        if name_match:
            name_match = first_token[name_match.start():name_match.end()]
            if male_names_rgx.match(name_match):
                new_genders.append({'source':'name', 'gender':'male'})
            elif female_names_rgx.match(name_match): # TODO: Should this be else instead?
                new_genders.append({'source':'name', 'gender':'female'})
            else: # Does this happen?
                raise("When searching name: all_names regex hit but male_names or female_names regexes did not!")

    # Regex match screen_name
    m = screen_name_rgx.match(screen_name)
    if m:
        nm = screen_name[m.start():m.end()]
        if nm in all_male_names:
            new_genders.append({'source':'screen_name', 'gender':'male'})
        else:
            new_genders.append({'source':'screen_name', 'gender':'female'})

    # Regex match description
    m = description_male_rgx.search(description)
    f = description_female_rgx.search(description)
    if m:
        new_genders.append({'source':'description', 'gender':'male'})
    if f:
        new_genders.append({'source':'description', 'gender':'female'})

    # Write to database
    user_col.update_one({"_id": id_str}, {"$set": {"genders": new_genders}})

print("Processed {} users in total.".format(str(total_processed)))
