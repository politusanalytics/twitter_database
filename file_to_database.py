import pymongo
import gzip
import json
import sys
from common import insert_if_does_not_exist

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]

input_filename = sys.argv[1] # currently accepts only gz files

with gzip.open(input_filename, "rb") as f:
    for user in f:
        user = json.loads(user)
        curr_user_tweets = []
        for tweet_obj in user["tweets"]:
            if tweet_obj["type"] == "original":
                curr_user_tweet = {"type": tweet_obj["type"], "date": tweet_obj["twt_date"]}
                to_be_inserted = {"_id": tweet_obj["twt_id_str"], "text": tweet_obj["twt_txt"],
                                  "date": tweet_obj["twt_date"],
                                  #"senti": tweet_obj["senti"], "n_ent": tweet_obj["n_ent"]
                                 }
                insert_if_does_not_exist(tweet_col, to_be_inserted)
            elif tweet_obj["type"] in ["retweet", "fav"]:
                curr_user_tweet = {"type": tweet_obj["type"], "date": tweet_obj["ref_twt_date"]}
                to_be_inserted = {"_id": tweet_obj["ref_twt_id_str"], "text": tweet_obj["ref_twt_txt"],
                                  "date": tweet_obj["ref_twt_date"],
                                  #"senti": tweet_obj["ref_senti"], "n_ent": tweet_obj["ref_n_ent"]
                                 }
                insert_if_does_not_exist(tweet_col, to_be_inserted)
            elif tweet_obj["type"] in ["quote", "reply"]:
                curr_user_tweet = {"type": tweet_obj["type"], "date": tweet_obj["twt_date"], "ref_date": tweet_obj["ref_twt_date"]}
                to_be_inserted = {"_id": tweet_obj["twt_id_str"], "text": tweet_obj["twt_txt"],
                                  "date": tweet_obj["twt_date"],
                                  #"senti": tweet_obj["senti"], "n_ent": tweet_obj["n_ent"]
                                 }
                to_be_inserted_ref = {"_id": tweet_obj["ref_twt_id_str"], "text": tweet_obj["ref_twt_txt"],
                                      "date": tweet_obj["ref_twt_date"],
                                      #"senti": tweet_obj["ref_senti"], "n_ent": tweet_obj["ref_n_ent"]
                                     }
                insert_if_does_not_exist(tweet_col, to_be_inserted)
                insert_if_does_not_exist(tweet_col, to_be_inserted_ref)

            curr_user_tweets.append(curr_user_tweet)

        insert_if_does_not_exist(user_col, {"_id": user["id_str"], "location": user["location"],
                                            "description": user["description"], "name": user["name"],
                                            "screen_name": user["screen_name"], "created_at": user["created_at"],
                                            "province_codes":user["province_codes"], "genders": user["genders"],
                                            "following": user["following"], "followers": user["followers"],
                                            "tweets": curr_user_tweets, "followers_count": user["followers_count"],
                                            "following_count": user["following_count"], "pp": user["pp"],
                                            "downloaded": user["downloaded"], "demog_pred": user["demog_pred"]})
