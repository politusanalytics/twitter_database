from __future__ import annotations
import pymongo
from typing import Optional, Dict, List, Any
import dateutil.parser

"""
This file currently contains possible use cases for the database. It will probably be converted
into an API along the way.
"""

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]

# For typing
mongo_result = List[Dict]

def write_results_to_file(result: mongo_result, out_filename: str) -> None:
    with open(out_filename, "w", encoding="utf-8") as f:
        for row in result:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

def print_result(result: mongo_result, row_limit: Optional[int] = 1000) -> None:
    for i, row in enumerate(result):
        print(row)
        if i == row_limit:
            break


def insert_one_if_does_not_exist(collection: pymongo.collection.Collection, to_be_inserted: Dict) -> None:
    if not collection.find_one({"_id": to_be_inserted["_id"]}):
        collection.insert_one(to_be_inserted)

def insert_one_or_replace(collection: pymongo.collection.Collection, to_be_inserted: Dict) -> None:
    """
    Directly replace the row if given _id exists. Note that this will completely overwrite the existing
    data.
    """
    collection.replaceOne({"_id": to_be_inserted["_id"]}, to_be_inserted, {upsert: true})

def insert_one_or_update(collection: pymongo.collection.Collection, to_be_updated: Dict) -> None:
    """
    Update the row if given _id exists. Note that this will only overwrite given columns in the
    to_be_updated, and write new columns.
    """
    collection.update_one({"_id": to_be_updated["_id"]}, {"$set": to_be_updated}, upsert=True)

def update_one(collection: pymongo.collection.Collection, to_be_updated: Dict) -> None:
    collection.update_one({"_id": to_be_updated["_id"]}, {"$set": to_be_updated})

# TODO: Should we divide this function further into update_by_regex, update_by_match, update_by_element
# etc?
def update_many_by_filter(collection: pymongo.collection.Collection, update_filter: Dict,
                          to_be_updated: Dict) -> None:
    collection.update_one(update_filter, {"$set": to_be_updated})


# Note that "_id" column is always returned
def get_all_tweets(columns_to_return: List[str] = []) -> mongo_result:
    result = tweet_col.find(projection=columns_to_return)
    return result

def get_all_users(columns_to_return: List[str] = []) -> mongo_result:
    result = user_col.find(projection=columns_to_return)
    return result

def get_users_with_tweet_type(tweet_type: str, columns_to_return: List[str] = []) -> mongo_result:
    result = user_col.find(filter={"tweets.type": tweet_type}, projection=columns_to_return)
    return result

def get_users_with_tweet_types(tweet_types: List[str], columns_to_return: List[str] = []) -> mongo_result:
    result = user_col.find(filter={"tweets.type": {"$in": tweet_types}}, projection=columns_to_return)
    return result

def get_users_with_ids(user_ids: List[str], columns_to_return: List[str] = []) -> mongo_result:
    result = user_col.find(filter={"_id": {"$in": user_ids}}, projection=columns_to_return)
    return result

def get_tweets_with_ids(tweet_ids: List[str], columns_to_return: List[str] = []) -> mongo_result:
    result = tweet_col.find(filter={"_id": {"$in": tweet_ids}}, projection=columns_to_return)
    return result

def get_tweets_between_dates(start_date: str, end_date: str, columns_to_return: List[str] = []) -> mongo_result:
    """
    Both start_date and end_date should be in format "YYYY-MM-DD".
    Also note that both dates are inclusive.
    """
    result = tweet_col.find(filter={"date": {"$gte": dateutil.parser.parse(start_date), "$lte": dateutil.parser.parse(end_date)}},
                            projection=columns_to_return)
    return result

def get_users_with_tweet_ids(tweet_ids: List[str], columns_to_return: List[str] = []) -> mongo_result:
    """
    tweet_ids can have multiple or a single id in it.
    """
    # result = user_col.find(filter={"tweets":
    #                                 {$elemMatch:
    #                                   {$or: [{"twt_id_str": {$in: tweet_ids}},
    #                                          {"ref_twt_id_str": {$in: tweet_ids}}]
    #                                   }
    #                                 }
    #                               },
    #                        projection=columns_to_return)
    result = user_col.find(filter={"$or": [{"tweets.twt_id_str": {"$in": tweet_ids}},
                                           {"tweets.ref_twt_id_str": {"$in": tweet_ids}},]},
                           projection=columns_to_return)
    return result

def get_users_with_tweets_between_dates(start_date: str, end_date: str,
                                        columns_to_return: List[str] = []) -> mongo_result:
    """
    May be used to get active users.
    """
    # # First way
    # result_tweets = get_tweets_between_dates(start_date, end_date)
    # result_tweet_ids = [tweet["_id"] for tweet in result_tweets]
    # result = get_users_with_tweet_ids(result_tweet_ids, columns_to_return=columns_to_return)

    # Second way
    result = user_col.find(filter={"tweets.date": {"$gte": dateutil.parser.parse(start_date), "$lte": dateutil.parser.parse(end_date)}},
                           projection=columns_to_return)

    return result

def filter_direct_match(list_or_filter: List[Any]) -> Dict:
    if len(list_or_filter) == 1:
        return list_or_filter[0]
    else:
        return {"$in": list_or_filter}

# both dates are inclusive, and with format 'YYYY-MM-DD'
def filter_between_dates(start_date: str, end_date: str) -> Dict:
    return {"$gte": dateutil.parser.parse(start_date), "$lte": dateutil.parser.parse(end_date)}

def filter_regex(regex_pattern: str, options: str = "si") -> Dict:
    return {"$regex": regex_pattern, "options": "<{}>".format(options)}

def generic_get_users(ids: Optional[List[str]] = None, locations: Optional[List[str]] = None,
                      description: Optional[str] = None, name: Optional[str] = None,
                      screen_name: Optional[str] = None, user_creation_date=None,
                      pcode: Optional[List[int]] = None, following: Optional[List[str]] = None,
                      followers: Optional[List[str]] = None, tweet_types: Optional[List[str]] = None,
                      tweet_date=None, tweet_ids: Optional[List[str]] = None,
                      min_followers_count: Optional[int] = None, max_followers_count: Optional[int] = None,
                      columns_to_return: Optional[List[str]] = []) -> mongo_result:
    """
    - ids: User id or ids.
    - locations: This is the location(s) of the user.
    - description: Must be a regex pattern.
    - name: Must be a regex pattern.
    - screen_name: Must be a regex pattern.
    - user_creation_date: Must be a list of two elements, with starting date and ending date.
    - pcode: This represent the province code(s) of the location of the user.
    - following: User id or ids.
    - followers: User id or ids.
    - tweet_types: Tweet type or types.
    - tweet_date: Must be a list of two elements, with starting date and ending date.
    - tweet_ids: Tweet id or ids.
    - min_followers_count: Minimum number of followers of the user.
    - max_followers_count: Maximum number of followers of the user.
    """

    if (tweet_ids is not None) and ((tweet_types is not None) or (tweet_date is not None)):
        raise("You would not need tweet_types or tweet_date filter if you have tweet_ids!")
    if (tweet_date is not None) and (len(tweet_date) != 2):
        raise("Length of tweet_date must be 2!")

    curr_filters = {}

    if (ids is not None) and (len(ids) != 0):
        curr_filters["_id"] = filter_direct_match(ids)
    if (locations is not None) and len(locations) != 0:
        curr_filters["location"] = filter_direct_match(locations)
    if user_creation_date is not None:
        curr_filters["created_at"] = filter_between_dates(user_creation_date[0], user_creation_date[1])
    if pcode is not None:
        curr_filters["province_codes.pcode"] = filter_direct_match(pcode)
    if (following is not None) and len(following) != 0:
        curr_filters["following"] = filter_direct_match(following)
    if (followers is not None) and len(followers) != 0:
        curr_filters["followers"] = filter_direct_match(followers)
    if (tweet_types is not None) and len(tweet_types) != 0:
        curr_filters["tweets.type"] = filter_direct_match(tweet_types)
    if (tweet_ids is not None) and len(tweet_ids) != 0:
        filt = filter_direct_match(tweet_ids)
        curr_filters["$or"] = [{"tweets.twt_id_str": filt}, {"tweets.ref_twt_id_str": filt}]
    if tweet_date is not None:
        curr_filters["tweets.date"] = filter_between_dates(tweet_date[0], tweet_date[1])
    if min_followers_count is not None:
        curr_filters["followers_count"] = {"$gte": min_followers_count} # Inclusive
    if max_followers_count is not None:
        curr_filters["followers_count"] = {"$lte": max_followers_count} # Inclusive
    if description is not None:
        curr_filters["description"] = filter_regex(description)
    if name is not None:
        curr_filters["name"] = filter_regex(name)
    if screen_name is not None:
        curr_filters["screen_name"] = filter_regex(screen_name)

    # QUESTION: If tweet_types is not None, should we change projection to only return tweets with the
    # given tweet_types?
    result = user_col.find(filter=curr_filters, projection=columns_to_return)
    return result

def generic_get_tweets(text: Optional[str] = None, date=None, ids: Optional[List[str]] = None,
                       columns_to_return: List[str] = []) -> mongo_result:
    # NOTE: This will probably get populated with various model prediction outputs.
    """
    - text: Must be a regex pattern.
    - date: Must be a list of two elements, with starting date and ending date.
    - ids: Tweet id or ids.
    """

    if (ids is not None) and ((text is not None) or (date is not None)):
        raise("You would not need text or date filter if you have ids!")
    if (date is not None) and (len(date) != 2):
        raise("Length of date must be 2!")

    curr_filters = {}

    if (ids is not None) and (len(ids) != 0):
        curr_filters["_id"] = filter_direct_match(ids)
    if date is not None:
        curr_filters["date"] = filter_between_dates(date[0], date[1])
    if text is not None:
        curr_filters["text"] = filter_regex(text)

    result = tweet_col.find(filter=curr_filters, projection=columns_to_return)
    return result


if __name__ == "__main__":
    # Necessary stuff for testing
    with open("test/test_tweet_ids.txt", "r") as f:
        test_tweet_ids = [str(tweet_id) for tweet_id in f.read().split(",")]
    with open("test/test_user_ids.txt", "r") as f:
        test_user_ids = [str(user_id) for user_id in f.read().split(",")]

    # Test stuff
    result1 = get_all_tweets()
    print("get_all_tweets:")
    print_result(result1, row_limit=3)
    print("---------------")
    result1 = generic_get_tweets()
    print("generic get_all_tweets:")
    print_result(result1, row_limit=3)
    print("===============")
    result2 = get_users_with_tweet_ids(test_tweet_ids)
    print("get_users_with_tweet_ids:")
    print_result(result2, row_limit=3)
    print("---------------")
    result2 = generic_get_users(tweet_ids=test_tweet_ids)
    print("generic get_users_with_tweet_ids:")
    print_result(result2, row_limit=3)
    print("===============")
    result3 = get_tweets_between_dates("2020-07-12", "2021-03-21")
    print("get_tweets_between_dates:")
    print_result(result3, row_limit=3)
    print("---------------")
    result3 = generic_get_tweets(date=["2020-07-12", "2021-03-21"])
    print("generic get_tweets_between_dates:")
    print_result(result3, row_limit=3)
    print("===============")
    result4 = get_users_with_tweets_between_dates("2020-07-12", "2021-03-21")
    print("get_users_with_tweets_between_dates:")
    print_result(result4, row_limit=3)
    print("---------------")
    result4 = generic_get_users(tweet_date=["2020-07-12", "2021-03-21"])
    print("generic get_users_with_tweets_between_dates:")
    print_result(result4, row_limit=3)
    print("===============")
    result5 = get_tweets_with_ids(test_tweet_ids)
    print("get_tweets_with_ids:")
    print_result(result5, row_limit=3)
    print("---------------")
    result5 = generic_get_tweets(ids=test_tweet_ids)
    print("generic get_tweets_with_ids:")
    print_result(result5, row_limit=3)
    print("===============")
    result6 = get_users_with_ids(test_user_ids)
    print("get_users_with_ids:")
    print_result(result6, row_limit=3)
    print("---------------")
    result6 = generic_get_users(ids=test_user_ids)
    print("generic get_users_with_ids:")
    print_result(result6, row_limit=3)
    print("===============")
    result7 = get_users_with_tweet_types(["fav", "quote", "original"])
    print("get_users_with_tweet_types:")
    print_result(result7, row_limit=3)
    print("---------------")
    result7 = generic_get_users(tweet_types=["fav", "quote", "original"])
    print("generic get_users_with_tweet_types:")
    print_result(result7, row_limit=3)
    print("===============")
    result8 = get_users_with_tweet_type("original")
    print("get_users_with_tweet_type:")
    print_result(result8, row_limit=3)
    print("---------------")
    result8 = generic_get_users(tweet_types=["original"])
    print("generic get_users_with_tweet_type:")
    print_result(result8, row_limit=3)
    print("===============")

    result9 = generic_get_users(user_creation_date=["2019-02-12", "2022-03-21"])
    print("generic user_creation_date:")
    print_result(result9, row_limit=3)
    print("===============")
    result10 = generic_get_users(pcode=[34])
    print("generic pcode:")
    print_result(result10, row_limit=3)
    print("===============")
    result11 = generic_get_users(min_followers_count=5, max_followers_count=100)
    print("generic min and max followers_count:")
    print_result(result11, row_limit=3)
    print("===============")
