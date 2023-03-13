import pymongo
import pandas as pd
from collections import Counter

out_filename = "exported_users.csv"
out_file = open(out_filename, "w")
write_batchsize = 1024 # write in intervals

age_groups = ["<=18", "19-29", "30-39", ">=40"]
ideology_1_labels = ["turkish_nationalism", "conservatism", "islamism", "liberalism", "kemalism"]
ideology_2_labels = ["social_democracy", "socialism", "feminism", "environmentalism",
                     "kurdish_national_movement", "secularism"]
welfare_labels = ["social_policy", "labour_and_employment", "education", "health_and_public_health",
                  "disability", "housing"]
democracy_labels = ["elections_and_voting", "justice_system", "human_rights", "regime_and_constitution",
                    "kurdish_question"]
big5_labels = ["internal_affairs", "national_defense", "corruption", "foreign_affairs", "economy"]
stance_labels = ["pro", "against", "neutral"]
pred_column_names = ["total"] + \
                    ["stance_"+lab for lab in stance_labels] + \
                    ["ide_"+lab for lab in ideology_1_labels] + \
                    ["ide_"+lab for lab in ideology_2_labels] + \
                    ["topic_"+lab for lab in welfare_labels] + \
                    ["topic_"+lab for lab in democracy_labels] + \
                    ["topic_"+lab for lab in big5_labels]

years = ["2018", "2019", "2020", "2021", "2022"]
months = ["0" + str(i) for i in range(1,10)] + [str(i) for i in range(10,13)]
unique_dates = []
for year in years:
    for month in months:
        unique_dates.append("{}-{}".format(year, month))

weeks = ["0" + str(i) for i in range(1,10)] + [str(i) for i in range(10,11)] # last data is from week 10
for week in weeks:
    unique_dates.append("2023-week{}".format(week))

pred_columns = ["{}_{}".format(date, col_name) for date in unique_dates for col_name in pred_column_names]
all_columns = ["id_str", "gender", "age_group", "location", "total_tweet_num"] + pred_columns
out_file.write(",".join(all_columns) + "\n")

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]

# 1013638 users have province_codes and are not organizations
# query = {"$or": [{"demog_pred_full.isOrg": {"$lte": 0.5}}, {"demog_pred_txt.isOrg": {"$lte": 0.5}}], "province_codes": {"$nin": [None, []]}}

# 939705 users have province_codes and are not organizations
query = {"province_codes": {"$nin": [None, []]}, "demog_pred_full.isOrg": {"$lte": 0.5}}
columns_to_return = ["_id", "tweets", "demog_pred_full", "demog_pred_txt", "province_codes"]
result = user_col.find(query, columns_to_return)

# unique_dates = []
write_idx = 0
to_be_written = ""
missing_dates = []
for user_idx, user in enumerate(result):
    curr_write = []

    # get age group and gender
    if user.get("demog_pred_full", "") != "":
        gender = "female" if user["demog_pred_full"]["isFemale"] >= 0.5 else "male"
        curr_age_preds = user["demog_pred_full"]["age"]
        age_group = age_groups[curr_age_preds.index(max(curr_age_preds))]
    elif user.get("demog_pred_txt", "") != "":
        gender = "female" if user["demog_pred_txt"]["isFemale"] >= 0.5 else "male"
        curr_age_preds = user["demog_pred_txt"]["age"]
        age_group = age_groups[curr_age_preds.index(max(curr_age_preds))]
    else:
        raise("No demog_pred for {}!".format(user["_id"]))

    curr_write.append(user["_id"])
    curr_write.append(gender)
    curr_write.append(age_group)

    # get possible locations
    province_codes = {"location": [], "description": [], "screen_name": []}
    for c in user["province_codes"]:
        province_codes[c["source"]].append(str(c["pcode"]))
    if len(province_codes["location"]) > 0:
        out_pcode = Counter(province_codes["location"]).most_common()[0][0]
    elif len(province_codes["description"]) > 0:
        out_pcode = Counter(province_codes["description"]).most_common()[0][0]
    elif len(province_codes["screen_name"]) > 0:
        out_pcode = Counter(province_codes["screen_name"]).most_common()[0][0]
    else:
        raise("Empty province_codes!")
    curr_write.append(out_pcode)

    # get curr user's tweets' predictions
    tweet_preds = {}
    results = tweet_col.find({"_id": {"$in": [tweet["id"] for tweet in user["tweets"]]}}, ["ideology_1", "ideology_2", "welfare", "democracy", "big5", "erdogan_stance"])
    for res in results:
        tweet_preds[res["_id"]] = (res.get("ideology_1", []) + res.get("ideology_2", []), res.get("welfare", []) + res.get("democracy", []) + res.get("big5", []), res.get("erdogan_stance", ""))

    # process tweets
    curr_write.append(len(user["tweets"]))

    tweets_dict = {}
    for col in pred_columns:
        tweets_dict[col] = 0

    for tweet in user["tweets"]:
        # Get date
        date = tweet["date"].strftime("%Y-%m")
        if int(date[:4]) < 2018:
            continue
        elif date[:4] == "2023":
            date = tweet["date"].strftime("%Y-week%V")

        if date not in unique_dates:
            # print(unique_dates)
            if date not in missing_dates:
                print("Tweet date {} is not in unique_dates!".format(date))
                missing_dates.append(date)
            continue

        tweets_dict[date+"_total"] += 1

        # ideology and topics
        ideologies, topics, stance = tweet_preds.get(tweet["id"], ([], [], ""))
        for ide in ideologies:
            ide = date + "_ide_" + ide
            tweets_dict[ide] += 1
        for topic in topics:
            topic = date + "_topic_" + topic
            tweets_dict[topic] += 1

        # Stance
        if stance != "":
            stance = date + "_stance_" + stance
            tweets_dict[stance] += 1


    # Write
    for col in pred_columns: # need to write ordered
        curr_write.append(tweets_dict[col])
    to_be_written += ",".join([str(elem) for elem in curr_write]) + "\n"
    write_idx += 1

    if write_idx == write_batchsize:
        out_file.write(to_be_written)
        write_idx = 0
        to_be_written = ""


if write_idx > 0:
    out_file.write(to_be_written)

out_file.close()

# paste -d',' <(cut -d',' -f -5 exported_users.csv) <(cut -d',' -f 1866- exported_users.csv) > exported_users_after_2023.csv
