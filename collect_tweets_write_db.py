import os
import sys
import pandas as pd
import tweepy
import json
import time
import datetime
import pymongo
from common import date_converter, insert_one_if_does_not_exist, insert_one_or_update
import shutil

today = datetime.datetime.today()
yesterday = today - datetime.timedelta(1)

dates = pd.date_range(start=yesterday.date(), end=today.date(), freq="min")
print(f"Collecting tweets from {dates[0]} to {dates[-1]}")

def date_converter_twitter_format(date):
    return str(date).split()[0] + "T" + str(date).split()[1] + "Z"

def type_converter(tweet_type):
    convert_dict = {"retweeted":"retweet", "replied_to":"reply", "quoted":"quote"}
    return convert_dict[tweet_type]

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]
other_users_col = db["other_users"]

# Create data directories
if "data" not in os.listdir():
    os.mkdir("data")
if "data_moved_to_db" not in os.listdir():
    os.mkdir("data_moved_to_db")

# Tweepy Authentication
with open("bearer_tokens.txt") as f:
    bearer_tokens = f.read().split("\n")
BEARER_TOKEN_NO = (today.day % 2)
bearer_token = bearer_tokens[int(BEARER_TOKEN_NO)]
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

"""
There will be three steps:
1- Collect Tweets, write to json files in "data" folder
2- Move collected tweets & users to database
3- Move other users to other_users collection
"""
####################

## Collect tweets
query = 'place_country:TR lang:tr'
print(f'Query: "{query}"')

for i in range(0, len(dates)-1):
    try:
        time.sleep(1)
        start_time = date_converter_twitter_format(dates[i])
        end_time = date_converter_twitter_format(dates[i+1])
        response = client.search_all_tweets(query = query,
                                            expansions=["author_id", "referenced_tweets.id", "edit_history_tweet_ids",
                                                        "in_reply_to_user_id", "attachments.media_keys", "attachments.poll_ids",
                                                        "geo.place_id", "entities.mentions.username", "referenced_tweets.id.author_id"],
                                            tweet_fields=["id", "text", "attachments", "author_id", "context_annotations",
                                                        "conversation_id", "created_at", "entities", "geo",
                                                        "in_reply_to_user_id", "lang", "possibly_sensitive",
                                                        "public_metrics", "referenced_tweets",
                                                        "reply_settings", "source", "withheld"],
                                            user_fields=["id", "name", "username", "created_at", "description", "entities",
                                                        "location", "pinned_tweet_id", "profile_image_url", "protected",
                                                        "public_metrics", "url", "verified", "withheld"],
                                            media_fields=["media_key", "type", "url", "duration_ms", "height",
                                                        "non_public_metrics", "organic_metrics", "preview_image_url",
                                                        "promoted_metrics", "public_metrics", "width", "alt_text", "variants"],
                                            place_fields=["full_name", "id", "contained_within", "country", "country_code",
                                                        "geo", "name", "place_type"],
                                            poll_fields=["id", "options", "duration_minutes", "end_datetime", "voting_status"],
                                            start_time=start_time,
                                            end_time=end_time,
                                            max_results=100)
        data = [tweet.data for tweet in response.data]
        includes = {}
        for key in response.includes.keys():
            includes[key] = [item.data for item in response.includes[key]]
        errors = response.errors
        response_dict = {"data":data, "includes":includes, "errors":errors}
        with open(f"data/tweets_{start_time.replace(':', '-')}_{end_time.replace(':', '-')}.json", "a+") as f:
            f.write(json.dumps(response_dict))
        print(f"{i:,}/{len(dates):,} | ({i/len(dates)*100:.2f}%)- Start Time: {start_time} | End Time: {end_time} | # of Tweets: {len(data)}")

    except Exception as e:
        print(f"{i:,}- {e}")

print("Tweets are collected!!!\n")

####################

## Move collected tweets & users to database
# Read response objects
files = [f"data/{file}" for file in os.listdir("data")]
file_count = len(files)
print(f"Response File Count: {file_count}")

print("Moving tweets & users to database")
# Add users & tweets to database
for i, file in enumerate(files):
    if i % 100 == 0:
        print(f"{i:,}/{file_count:,} | {i/file_count*100:.2f}%")
    # Read response object
    with open(file) as f:
        response = json.loads(f.read())
    # Extract users & tweets from response, add tweets to user objects
    users = [user for user in response["includes"]["users"] if user["id"] in [tweet["author_id"] for tweet in response["includes"]["tweets"]]]
    tweets = response["includes"]["tweets"]
    tweet_errors = [error for error in response["errors"] if error["resource_type"] == "tweet"]

    geo_id_to_place_full_name = {place["id"]:place["full_name"] for place in response["includes"]["places"]}
    geo_id_to_country_code = {place["id"]:place["country_code"] for place in response["includes"]["places"]}
    tweet_id_to_tweet = {tweet["id"]:tweet for tweet in response["includes"]["tweets"]}

    for tweet in tweets:
        tweet["twt_txt"] = tweet.pop("text")
        tweet["twt_id_str"] = tweet.pop("id")
        date_string = tweet.pop("created_at").split("T")[0]
        tweet["twt_date"] = date_string[2:4] + date_string[5:7] + date_string[8:10]
        tweet["retweet_count"] = tweet["public_metrics"]["retweet_count"]
        tweet["reply_count"] = tweet["public_metrics"]["reply_count"]
        tweet["like_count"] = tweet["public_metrics"]["like_count"]
        tweet["quote_count"] = tweet["public_metrics"]["quote_count"]
        tweet["impression_count"] = tweet["public_metrics"]["impression_count"]
        
        if "referenced_tweets" in tweet.keys():
            try:
                tweet["type"] = type_converter(tweet["referenced_tweets"][0]["type"])
                tweet["ref_twt"] = tweet_id_to_tweet[tweet["referenced_tweets"][0]["id"]]
                _ = tweet.pop("referenced_tweets")
            except:
                tweet["type"] = type_converter(tweet["referenced_tweets"][0]["type"])
                tweet["ref_twt_id_str"] = tweet["referenced_tweets"][0]["id"]
                tweet["ref_twt_txt"] = ""
                tweet["ref_twt_date"] = ""
                _ = tweet.pop("referenced_tweets")
        else:
            tweet["type"] = "original"
            
        if "geo" in tweet.keys():
            try:
                tweet["place"] = geo_id_to_place_full_name[tweet["geo"]["place_id"]]
            except:
                pass
            try:
                tweet["country_code"] = geo_id_to_country_code[tweet["geo"]["place_id"]]
            except:
                pass
            _ = tweet.pop("geo")
            
        for key in ["attachments", "context_annotations", "edit_history_tweet_ids", "entities",
                    "in_reply_to_user_id", "possibly_sensitive", "reply_settings", "geo", "public_metrics"]:
            if key in tweet.keys():
                _ = tweet.pop(key)

    for tweet in tweets:
        if "ref_twt" in tweet.keys():
            for key in tweet["ref_twt"]:
                if key not in ["referenced_tweets", "ref_twt", "ref_twt_txt", "ref_twt_id_str", "ref_usr_id_str", "ref_twt_date"]:
                    tweet[f"ref_{key}"] = tweet["ref_twt"][key]
            _ = tweet.pop("ref_twt")
        if "ref_author_id" in tweet.keys():
            _ = tweet.pop("ref_author_id")

    for user in users:
        _ = user.pop("protected")
        _ = user.pop("verified")
        if "pinned_tweet_id" in user.keys():
            _ = user.pop("pinned_tweet_id")
        if "entities" in user.keys():
            _ = user.pop("entities")
        if "url" in user.keys():
            _ = user.pop("url")
        user["pp"] = '/'.join(user.pop("profile_image_url").split("/")[-2:])
        user["id_str"] = user.pop("id")
        user["screen_name"] = user.pop("username")
        public_metrics = user.pop("public_metrics")
        user["followers_count"] = public_metrics["followers_count"]
        user["following_count"] = public_metrics["following_count"]
        user["tweet_count"] = public_metrics["tweet_count"]
        user_tweets = [tweet for tweet in tweets if tweet["author_id"] == user["id_str"]]

        user["tweets"] = [{key:value for key, value in tweet.items() if key != "author_id"} for tweet in user_tweets]


    for user in users:
        user_id = user.pop("id_str")
        user["_id"] = user_id

        # Insert or update user (every key except "tweets")
        insert_one_or_update(collection=user_col, to_be_updated={key:value for key, value in user.items() if key != "tweets"} | {"tweets":[]})

        # Insert or update tweets
        curr_user_tweets = []
        for tweet_obj in user["tweets"]:
            # Original
            if tweet_obj["type"] == "original":
                curr_user_tweet = {"type": tweet_obj["type"], "id": tweet_obj["twt_id_str"], "date": date_converter(tweet_obj["twt_date"])}
                to_be_inserted = {"_id": tweet_obj["twt_id_str"],
                                  "text": tweet_obj["twt_txt"],
                                  "date": date_converter(tweet_obj["twt_date"])} | {key:value for key, value in tweet_obj.items() if key not in ["twt_id_str", "twt_txt", "twt_date", "type"]}
                insert_one_or_update(collection=tweet_col, to_be_updated=to_be_inserted)
            # Quote or Reply
            else:
                curr_user_tweet = {"type": tweet_obj["type"], "id":tweet_obj["twt_id_str"], "ref_id":tweet_obj["ref_twt_id_str"],
                                   "date": date_converter(tweet_obj["twt_date"]), "ref_date": date_converter(tweet_obj["ref_twt_date"])}
                to_be_inserted = {"_id": tweet_obj["twt_id_str"],
                                  "text": tweet_obj["twt_txt"],
                                  "date": date_converter(tweet_obj["twt_date"])} | {key:value for key, value in tweet_obj.items() if key not in ["twt_id_str", "twt_txt", "twt_date", "type"] and "ref" not in key}

                to_be_inserted_ref = {"_id": tweet_obj["ref_twt_id_str"],
                                      "text": tweet_obj["ref_twt_txt"],
                                      "date": date_converter(tweet_obj["ref_twt_date"])} | {key[4:]:value for key, value in tweet_obj.items() if key not in ["ref_twt_id_str", "ref_twt_txt", "ref_twt_date", "type", "ref_type"] and "ref" in key}
                insert_one_or_update(collection=tweet_col, to_be_updated=to_be_inserted)
                if to_be_inserted_ref["text"] != "":
                    insert_one_or_update(collection=tweet_col, to_be_updated=to_be_inserted_ref)
                else:
                    insert_one_if_does_not_exist(collection=tweet_col, to_be_inserted=to_be_inserted_ref)

            curr_user_tweets.append(curr_user_tweet)

        tweets_in_db = user_col.find_one({"_id":user_id})["tweets"]
        
        combined_tweets = curr_user_tweets + tweets_in_db
        unique_curr_user_tweets = list({v['id']:v for v in combined_tweets}.values())

        user_col.update_one({"_id":user_id}, {"$set": {'tweets_updated':date_converter(today.strftime("%y%m%d"))}})
        user_col.update_one({"_id":user_id}, {"$set": {'downloaded':date_converter(today.strftime("%y%m%d"))}})
        user_col.update_one({"_id":user_id}, {"$set": {'tweets':unique_curr_user_tweets}})

print(f"{file_count:,}/{file_count:,} | {file_count/file_count*100:.2f}%")
print("Tweets & users moved to database!!!\n")

####################

## Move other users to other_users collection
# Read response objects
files = [f"data/{file}" for file in os.listdir("data")]
file_count = len(files)
print(f"Response File Count: {file_count}")

for i, file in enumerate(files):
    if i % 1_000 == 0:
        print(f"{i:,}/{file_count:,} | {i/file_count*100:.2f}%")
    with open(file) as f:
        response = json.loads(f.read())
        users = response["includes"]["users"]

        for user in users:
            if user_col.find_one({"_id":user["id"]}):
                continue
            else:
                user_dict = {"_id":user["id"], "screen_name":user["username"], "followers_count":user["public_metrics"]["followers_count"],
                                            "following_count":user["public_metrics"]["following_count"],
                                            "tweet_count":user["public_metrics"]["tweet_count"],
                                            "pp":'/'.join(user["profile_image_url"].split("/")[-2:])
                                            } | {key:value for key, value in user.items() if key not in ["id", "public_metrics", "entities", "pinned_tweet_id",
                                                                                                         "profile_image_url", "protected", "url", "verified", "username",
                                                                                                         "withheld"]}
                if other_users_col.find_one({"_id":user["id"]}):
                    source = other_users_col.find_one({"_id":user["id"]})["source"]
                    source = ','.join(sorted(set(source.split(",") + ["search_tweets"])))
                    insert_one_or_update(other_users_col, user_dict | {"source":source})
                else:
                    insert_one_if_does_not_exist(other_users_col, user_dict | {"source":"search_tweets"})
    shutil.move(file, "data_moved_to_db")
print(f"{file_count:,}/{file_count:,} | {file_count/file_count*100:.2f}%")
print("Other users moved to database!!!")
