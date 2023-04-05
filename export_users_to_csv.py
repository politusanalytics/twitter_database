import pymongo
from collections import Counter

out_filename = "exported_users.csv"
out_file = open(out_filename, "w")
write_batchsize = 1024 # write in intervals

age_groups = ["<=18", "19-29", "30-39", ">=40"]

ideology_1_labels = ["turkish_nationalism", "conservatism", "islamism", "liberalism", "kemalism"]
ideology_2_labels = ["social_democracy", "socialism", "feminism", "environmentalism",
                     "kurdish_national_movement", "secularism"]
ideology_labels = ["ide_"+ide for ide in ideology_1_labels] + ["ide_"+ide for ide in ideology_2_labels]

welfare_labels = ["social_policy", "labour_and_employment", "education", "health_and_public_health",
                  "disability", "housing"]
democracy_labels = ["elections_and_voting", "justice_system", "human_rights", "regime_and_constitution",
                    "kurdish_question"]
big5_labels = ["internal_affairs", "national_defense", "corruption", "foreign_affairs", "economy"]
topic_labels = welfare_labels + democracy_labels + big5_labels

emotion_labels = ["notr", "mutluluk", "sevgi", "umut", "minnet", "saskinlik", "uzuntu", "kaygi",
                  "korku", "umutsuzluk", "utanc", "pismanlik", "ofke", "igrenme", "arzu",
                  "onaylama", "onaylamama"]

topic_emotion_combinations = ["umut-social_policy", "umut-human_rights", "umut-economy",
                              "umut-education", "umut-health_and_public_health", "umut-justice_system",
                              "minnet-social_policy", "minnet-deprem", "uzuntu-deprem", "kaygi-deprem",
                              "korku-social_policy", "korku-human_rights", "korku-economy",
                              "korku-education", "korku-health_and_public_health", "korku-justice_system",
                              "korku-deprem", "umutsuzluk-social_policy", "umutsuzluk-human_rights",
                              "umutsuzluk-education", "umutsuzluk-health_and_public_health",
                              "umutsuzluk-justice_system", "umutsuzluk-economy", "ofke-human_rights",
                              "ofke-economy", "ofke-education", "ofke-health_and_public_health",
                              "ofke-justice_system", "ofke-deprem", "arzu-social_policy",
                              "arzu-human_rights", "arzu-education", "arzu-health_and_public_health",
                              "arzu-justice_system"]
topic_stance_combinations = ["kk_pro-social_policy", "kk_against-social_policy", "kk_neutral-social_policy",
                             "kk_pro-human_rights", "kk_against-human_rights", "kk_neutral-human_rights",
                             "kk_pro-economy", "kk_against-economy", "kk_neutral-economy",
                             "kk_pro-justice_system", "kk_against-justice_system", "kk_neutral-justice_system",
                             "erdogan_pro-social_policy", "erdogan_against-social_policy", "erdogan_neutral-social_policy",
                             "erdogan_pro-human_rights", "erdogan_against-human_rights", "erdogan_neutral-human_rights",
                             "erdogan_pro-economy", "erdogan_against-economy", "erdogan_neutral-economy",
                             "erdogan_pro-justice_system", "erdogan_against-justice_system", "erdogan_neutral-justice_system"]

stance_labels = ["pro", "against", "neutral"]

pred_column_names = ["total"] + \
                    ["erdogan_"+lab for lab in stance_labels] + \
                    ["kk_"+lab for lab in stance_labels] + \
                    ["topic_"+lab for lab in topic_labels] + \
                    ["emotion_"+lab for lab in emotion_labels] + \
                    topic_emotion_combinations + topic_stance_combinations

years = ["2022"]
months = ["0" + str(i) for i in range(1,10)] + [str(i) for i in range(10,13)]
unique_dates = []
for year in years:
    for month in months:
        unique_dates.append("{}-{}".format(year, month))

# weeks = ["0" + str(i) for i in range(1,10)] + [str(i) for i in range(10,11)] # last data is from week 10
# for week in weeks:
#     unique_dates.append("2023-week{}".format(week))
months = ["0" + str(i) for i in range(1,4)]
for month in months:
    unique_dates.append("2023-{}".format(month))

pred_columns = ["{}_{}".format(date, col_name) for date in unique_dates for col_name in pred_column_names]
all_columns = ["id_str", "gender", "age_group", "location", "total_tweet_num"] + ideology_labels + pred_columns
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

# TODO: Nearly 1 million user here do not have any tweet after 2022. Check if there is a bug!!!

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
    results = tweet_col.find({"_id": {"$in": [tweet["id"] for tweet in user["tweets"]]}}, ["ideology_1", "ideology_2", "welfare", "democracy", "big5", "emotions", "erdogan_stance", "kk_stance"])
    for res in results:
        tweet_preds[res["_id"]] = (res.get("emotions", []), res.get("ideology_1", []) + res.get("ideology_2", []), res.get("welfare", []) + res.get("democracy", []) + res.get("big5", []), res.get("erdogan_stance", ""), res.get("kk_stance", ""))


    # process tweets
    curr_total_tweet_num = 0
    ide_dict = {}
    for ide in ideology_labels:
        ide_dict[ide] = 0
    tweets_dict = {}
    for col in pred_columns:
        tweets_dict[col] = 0

    for tweet in user["tweets"]:
        # Get date
        date = tweet["date"].strftime("%Y-%m")
        if int(date[:4]) < 2018:
            continue
        elif int(date[:4]) < 2022:
            # We only want ideologies for tweets between 2018 and 2022
            emotions, ideologies, topics, erdogan_stance, kk_stance = tweet_preds.get(tweet["id"], ([], [], [], "", ""))
            for ide in ideologies:
                ide = "ide_" + ide
                ide_dict[ide] += 1
            continue

        # elif date[:4] == "2023":
            # date = tweet["date"].strftime("%Y-week%V")

        if date not in unique_dates:
            # print(unique_dates)
            if date not in missing_dates:
                print("Tweet date {} is not in unique_dates!".format(date))
                missing_dates.append(date)
            continue

        curr_total_tweet_num += 1
        tweets_dict[date+"_total"] += 1

        # ideology, topic and emotions
        emotions, ideologies, topics, erdogan_stance, kk_stance = tweet_preds.get(tweet["id"], ([], [], [], "", ""))

        for ide in ideologies:
            ide = "ide_" + ide
            ide_dict[ide] += 1

        curr_emotions = []
        for emo in emotions:
            curr_emotions.append(emo)
            emo = date + "_emotion_" + emo
            tweets_dict[emo] += 1
        curr_topics = []
        for topic in topics:
            curr_topics.append(topic)
            topic = date + "_topic_" + topic
            tweets_dict[topic] += 1

        # for topic_emotions
        for emo in curr_emotions:
            for topic in curr_topics:
                curr_key = date + "_" + emo + "-" + topic
                if tweets_dict.get(curr_key, "") != "":
                    tweets_dict[curr_key] += 1

        # Erdogan stance
        if erdogan_stance != "":
            erdogan_stance = date + "_erdogan_" + erdogan_stance
            tweets_dict[erdogan_stance] += 1
            # for topic_stance
            for topic in curr_topics:
                curr_key = erdogan_stance+"-"+topic
                if tweets_dict.get(curr_key, "") != "":
                    tweets_dict[curr_key] += 1

        # Kilicdar stance
        if kk_stance != "":
            kk_stance = date + "_kk_" + kk_stance
            tweets_dict[kk_stance] += 1
            # for topic_stance
            for topic in curr_topics:
                curr_key = kk_stance+"-"+topic
                if tweets_dict.get(curr_key, "") != "":
                    tweets_dict[curr_key] += 1



    # Write
    curr_write.append(curr_total_tweet_num)
    for col in ideology_labels: # need to write ordered
        curr_write.append(ide_dict[col])

    for col in pred_columns: # need to write ordered
        curr_write.append(tweets_dict[col])

    assert(len(curr_write) == len(all_columns))

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
