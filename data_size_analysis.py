import pymongo
import dateutil.parser

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]


total_users_count = user_col.count_documents({})
total_tweets_count = tweet_col.count_documents({})
avg_user_size = db.command("collstats", "users")["avgObjSize"] # in bytes
avg_tweet_size = db.command("collstats", "tweets")["avgObjSize"] # in bytes

# total_text_size = list(tweet_col.aggregate([{"$group": {"_id": null, "textSize": {"$sum": {"$bsonSize": {"text": 1}}}}}]))[0]["textSize"]

print("Total base user size needed(expected # of users is 10 million) in GB is {}".format((avg_user_size * 10000000)/1024/1024/1024))
print("Total base tweet size needed(expected # of tweets is 200 million) in GB is {}".format((avg_tweet_size * 200000000)/1024/1024/1024))

months = ["2022-07-01", "2022-08-01", "2022-09-01", "2022-10-01", "2022-11-01", "2022-12-01"]
monthly_tweet_sizes_in_mb = []
for month_idx in range(len(months)-1):
    curr_filter = {"date": {"$gte": dateutil.parser.parse(months[month_idx]), "$lt": dateutil.parser.parse(months[month_idx+1])}}
    curr_month_tweets_count = tweet_col.count_documents(curr_filter)
    curr_month_tweet_size_in_mb = (curr_month_tweets_count*avg_tweet_size)/1024/1024
    monthly_tweet_sizes_in_mb.append(curr_month_tweet_size_in_mb)

print("Montly average tweet data size in MB is {}".format(sum(monthly_tweet_sizes_in_mb)/len(months)))
