import pymongo
import gzip
import json

"""
find examples:
- find row with id: col.find_one({"_id": id})
- find row with id, but return only its location and name: `col.find_one({"_id": id}, ["location", "name"])`
or to not return _id as well, you can do: `col.find_one({"_id": id}, {"_id":False, "location":True, "name":True})`
- find rows with tweets of type fav and return only these tweets: `col.find({"tweets.type": "fav"}, {"_id":False, "tweets":True})`
note that unlike find_one, you need to iterate the return value here.
"""

def insert_if_does_not_exist(collection, to_be_inserted):
    if not collection.find_one({"_id": to_be_inserted["_id"]}):
        collection.insert_one(to_be_inserted)

def insert_or_update(collection, to_be_inserted):
    collection.replaceOne({"_id": to_be_inserted["_id"]}, to_be_inserted, {upsert: true})

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]

# ...
# Collected info about users
# Collected their tweets as well
# ...

with gzip.open("/data01/myardi/220920/province_gender_available_metadata_added-220920_combined_wPreds.txt.gz", "rb") as f:
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




#for user in collected_users:
#    # QUESTION: Do we want to add referred tweets to other users' tweets? What if we don't have
#    # the user in our collection?
#    curr_user_tweets = []
#    for tweet_obj in user["tweets"]:
#        curr_user_tweet = {"type": tweet_obj["type"], "date": tweet_obj["twt_date"]}
#        if tweet_obj.get("twt_id_str", ""): # twt_id_str is not present in retweets and favs
#            curr_user_tweet["twt_id"] = tweet_obj["twt_id_str"]
#            to_be_inserted = {"_id": tweet_obj["twt_id_str"], "text": tweet_obj["twt_txt"],
#                              "date": tweet_obj["twt_date"], "senti": tweet_obj["twt_txt"],
#                              "n_ent": tweet_obj["n_ent"]}
#            insert_if_does_not_exist(tweet_col, to_be_inserted)
#
#        # Note that this is not elif
#        if tweet_obj.get("ref_twt_id_str", ""): # ref_twt_id_str is not present in original tweets
#            curr_user_tweet["ref_twt_id"] = tweet_obj["ref_twt_id_str"]
#            # QUESTION: Is there a ref_twt_date? If not we have leave it null here
#            to_be_inserted = {"_id": tweet_obj["ref_twt_id_str"], "text": tweet_obj["ref_twt_txt"],
#                              "date": tweet_obj["ref_twt_date"], "senti": tweet_obj["ref_twt_txt"],
#                              "n_ent": tweet_obj["ref_n_ent"]}
#            insert_if_does_not_exist(tweet_col, to_be_inserted)
#
#        curr_user_tweets.append(curr_tweet_tweet)
#
#
#    insert_if_does_not_exist(user_col, {"_id": user["id_str"], "location": user["location"],
#                                        "description": user["description"], "name": user["name"],
#                                        "screen_name": user["screen_name"], "following": user["following"],
#                                        "followers": user["followers"],
#                                        "tweets": curr_user_tweets})
#
#
#if find_one({"_id": to_be_collected_user_id}):
#    continue
