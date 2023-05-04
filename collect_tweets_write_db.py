import os
import sys
import pandas as pd
import tweepy
import json
import datetime
import pymongo
import shutil
import sys
import random
import string
import dateutil.parser

random.seed(0)

def get_random_string(length=128):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

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
    bearer_tokens = [bearer_token for bearer_token in f.read().split("\n") if bearer_token]
BEARER_TOKEN_NO = int(sys.argv[1])
bearer_token = bearer_tokens[BEARER_TOKEN_NO-1]
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

# Specify user fields to be updated
user_fields = [
    'name', 'screen_name', 'description', 'location', 
    'followers_count', 'following_count', 'tweet_count',
    'pp', 'withheld', 'protected']

# Specify dates
today = datetime.datetime.today()
yesterday = today - datetime.timedelta(1)

dates = pd.date_range(start=yesterday.date(), end=today.date(), freq="5s")

bearer_token_count = len(bearer_tokens)
date_count_per_bearer_token = round(len(dates) / bearer_token_count) + 1

dates = dates[date_count_per_bearer_token*(BEARER_TOKEN_NO-1):date_count_per_bearer_token*BEARER_TOKEN_NO]

print(f"Collecting tweets from {dates[0]} to {dates[-1]} | freq: 5 seconds")

"""
There will be three steps:
1- Collect Tweets, write to json files in "data" folder
2- Move collected tweets & users to database
3- Move other users to other_users collection
"""
####################

# Collect Tweets
random_string = get_random_string(length=128)
query = f'-{random_string} lang:tr'
print(f'Query: "{query}"')

for i in range(0, len(dates)-1):
    try:
        start_time = date_converter_twitter_format(dates[i])
        end_time = date_converter_twitter_format(dates[i+1])
        response = client.search_recent_tweets(query = query,
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
    # If there are any tweets (i.e., response.data != None)
    if response["data"]:
        # response.data --> tweets
        tweets = response["data"]
        # If any tweet has "geo" field, response.includes will have "place"
        if "places" in response["includes"].keys():
            place_id_to_full_name = {place["id"]: place["full_name"] for place in response["includes"]["places"]}
            place_id_to_country_code = {place["id"]: place["country_code"] for place in response["includes"]["places"]}
        # If there are any referenced tweets --> ref_tweets
        if "tweets" in response["includes"].keys():
            ref_tweets = response["includes"]["tweets"]
            # Process referenced tweets
            for tweet in ref_tweets:
                for key in ["attachments", "context_annotations", "edit_history_tweet_ids", "entities",
                            "possibly_sensitive", "reply_settings",]:
                    if key in tweet.keys():
                        _ = tweet.pop(key)
                if "created_at" in tweet.keys():
                    tweet["date"] = dateutil.parser.parse(tweet.pop("created_at"))
                if "public_metrics" in tweet.keys():
                    tweet["retweet_count"] = tweet["public_metrics"]["retweet_count"]
                    tweet["reply_count"] = tweet["public_metrics"]["reply_count"]
                    tweet["like_count"] = tweet["public_metrics"]["like_count"]
                    tweet["quote_count"] = tweet["public_metrics"]["quote_count"]
                    tweet["impression_count"] = tweet["public_metrics"]["impression_count"]
                    _ = tweet.pop("public_metrics")
                if "geo" in tweet.keys():
                    try:
                        tweet["place"] = place_id_to_full_name[tweet["geo"]["place_id"]]
                    except:
                        pass
                    try:
                        tweet["country_code"] = place_id_to_country_code[tweet["geo"]["place_id"]]
                    except:
                        pass
                    _ = tweet.pop("geo")
                if "referenced_tweets" in tweet.keys():
                    _ = tweet.pop("referenced_tweets")
        # Process tweets
        for tweet in tweets:
            for key in ["attachments", "context_annotations", "edit_history_tweet_ids", "entities",
                        "possibly_sensitive", "reply_settings",]:
                if key in tweet.keys():
                    _ = tweet.pop(key)
            if "created_at" in tweet.keys():
                tweet["date"] = dateutil.parser.parse(tweet.pop("created_at"))
            if "public_metrics" in tweet.keys():
                tweet["retweet_count"] = tweet["public_metrics"]["retweet_count"]
                tweet["reply_count"] = tweet["public_metrics"]["reply_count"]
                tweet["like_count"] = tweet["public_metrics"]["like_count"]
                tweet["quote_count"] = tweet["public_metrics"]["quote_count"]
                tweet["impression_count"] = tweet["public_metrics"]["impression_count"]
                _ = tweet.pop("public_metrics")
            if "geo" in tweet.keys():
                try:
                    tweet["place"] = place_id_to_full_name[tweet["geo"]["place_id"]]
                except:
                    pass
                try:
                    tweet["country_code"] = place_id_to_country_code[tweet["geo"]["place_id"]]
                except:
                    pass
                _ = tweet.pop("geo")
            
            if "referenced_tweets" in tweet.keys():
                tweet["type"] = type_converter(tweet["referenced_tweets"][0]["type"])
                ref_tweet_id = tweet["referenced_tweets"][0]["id"]
                try:
                    ref_tweet = [tweet for tweet in ref_tweets if tweet["id"] == ref_tweet_id][0]
                    for key in ref_tweet.keys():
                        tweet[f"ref_{key}"] = ref_tweet[key]
                    _ = tweet.pop("referenced_tweets")
                except:
                    tweet["ref_id"] = ref_tweet_id
                    _ = tweet.pop("referenced_tweets")
            else:
                tweet["type"] = "original"

    users = [user for user in response["includes"]["users"] if user["id"] in [tweet["author_id"] for tweet in response["includes"]["tweets"]]]

    for user in users:
        user["created_at"] = dateutil.parser.parse(user.pop("created_at"))
        if "pinned_tweet_id" in user.keys():
            _ = user.pop("pinned_tweet_id")
        if "entities" in user.keys():
            _ = user.pop("entities")
        user["pp"] = '/'.join(user.pop("profile_image_url").split("/")[-2:])
        user["screen_name"] = user.pop("username")
        public_metrics = user.pop("public_metrics")
        user["followers_count"] = public_metrics["followers_count"]
        user["following_count"] = public_metrics["following_count"]
        user["tweet_count"] = public_metrics["tweet_count"]
        user_tweets = [tweet for tweet in tweets if tweet["author_id"] == user["id"]]
        user["tweets"] = [{key:value for key, value in tweet.items()} for tweet in user_tweets]
        user_id = user.pop("id")
        user["_id"] = user_id
        curr_user_tweets = []
        for tweet_obj in user["tweets"]:
            # Original
            if tweet_obj["type"] == "original":
                tweet_id = tweet_obj["id"]
                curr_user_tweet = {"type": tweet_obj["type"], "id": tweet_id, "date": tweet_obj["date"]}
                to_be_inserted = {"_id": tweet_id} | {key:value for key, value in tweet_obj.items() if key not in ["type", "id"]}
                if tweet_col.find_one({"_id":tweet_id}):
                    tweet_col.update_one(filter={"_id":tweet_id}, update={"$set":tweet_obj})
                else:
                    tweet_col.insert_one(to_be_inserted)
            # Quote, Reply or Retweet
            else:
                tweet_id = tweet_obj["id"]
                ref_tweet_id = tweet_obj["ref_id"]
                curr_user_tweet = {"type": tweet_obj["type"], "id":tweet_id, "ref_id":ref_tweet_id,
                                    "date": tweet_obj["date"]}
                if "ref_date" in tweet_obj.keys():
                    curr_user_tweet["ref_date"] = tweet_obj["ref_date"]
                to_be_inserted = {"_id": tweet_id} | {key:value for key, value in tweet_obj.items() if key not in ["type", "id"] and "ref" not in key}
                to_be_inserted_ref = {"_id": ref_tweet_id} | {key[4:]:value for key,value in tweet_obj.items() if key not in ["ref_type", "ref_id"] and "ref" in key}
                if tweet_col.find_one({"_id":tweet_id}):
                    tweet_col.update_one(filter={"_id":tweet_id}, update={"$set":to_be_inserted})
                else:
                    tweet_col.insert_one(to_be_inserted)
                if tweet_col.find_one({"_id":ref_tweet_id}):
                    tweet_col.update_one(filter={"_id":ref_tweet_id}, update={"$set":to_be_inserted_ref})
                else:
                    tweet_col.insert_one(to_be_inserted_ref)
            curr_user_tweets.append(curr_user_tweet)
        # Update user & tweets if already in database
        if user_col.find_one({"_id":user_id}):
            user_db = user_col.find_one({"_id":user_id})
            if "user_hist" in user_db.keys():
                user_hist = user_db["user_hist"]
            else:
                user_hist = []
            old_user_dict = {}
            for field in user_fields:
                if field in user_db.keys():
                    old_user_dict[field] = user_db[field]
            if "user_updated" in user_db.keys():
                old_user_dict["user_updated"] = user_db["user_updated"]
            user_hist.append(old_user_dict)
            user_col.update_one(filter={"_id":user_id}, update={"$set":{key:value for key, value in user.items() if key not in ["tweets"]} | {"user_hist":user_hist} | {"user_updated":today}})
            # Update user's tweets
            users_tweets_db = user_db["tweets"]
            combined_tweets = curr_user_tweets + [tweet for tweet in users_tweets_db]
            unique_curr_user_tweets = list({v['id']:v for v in combined_tweets}.values())
            user_col.update_one(filter={"_id":user_id}, update={"$set":{"tweets":unique_curr_user_tweets}})
        # Insert user if not in database
        else:
            user_id = user["_id"]
            user["user_hist"] = []
            user_dict = {}
            for field in user_fields:
                if field in user.keys():
                    user_dict[field] = user[field]
            user_dict["user_updated"] = today
            user["user_hist"].append(user_dict)
            user["user_updated"] = today
            user_col.insert_one({key:value for key, value in user.items() if key not in ["tweets"]})
            user_col.update_one(filter={"_id":user_id}, update={"$set":{"tweets":curr_user_tweets}})

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
        user_id = user["id"]
        if user_col.find_one({"_id":user_id}):
            continue
        else:
            user_dict = {"_id":user_id, "screen_name":user["username"], "created_at": dateutil.parser.parse(user["created_at"]),
                         "followers_count":user["public_metrics"]["followers_count"],
                         "following_count":user["public_metrics"]["following_count"],
                         "tweet_count":user["public_metrics"]["tweet_count"],
                         "pp":'/'.join(user["profile_image_url"].split("/")[-2:])
                         } | {key:value for key, value in user.items() if key not in ["id", "public_metrics", "entities", "pinned_tweet_id",
                                                                                      "profile_image_url", "username", "withheld", "created_at"]}
            if other_users_col.find_one({"_id":user_id}):
                source = other_users_col.find_one({"_id":user_id})["source"]
                source = ','.join(sorted(set(source.split(",") + ["search_tweets"])))
                other_users_col.update_one(filter={"_id":user_id}, update={"$set":user_dict | {"source": source}})
            else:
                other_users_col.insert_one(user_dict | {"source":"search_tweets"})
    shutil.move(file, "data_moved_to_db")
print(f"{file_count:,}/{file_count:,} | {file_count/file_count*100:.2f}%")
print("Other users moved to database!!!")
