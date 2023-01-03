# twitter_database
This repo consists of code regarding the creation of our twitter database and interaction with this database

## MongoDB data structure
There are two collections: users and tweets

### Users
* "_id": Id of the user
* "location": Location id for the user
* "description": Twitter description(bio) of the user
* "name": Twitter name of the user
* "screen_name": Twitter screen name of the user
* "created_at": The date account was created
* "province_codes": Province code of the user's city
* "genders": Gender of the user
* "following": The user ids that the user follows
* "followers": The user ids that follow the user
* "tweets": Tweets of the user. Each tweet is an object that contains "tweet_date", "type" and may contain "tweet_id" and/or "ref_tweet_id"
* "followers_count": Follower count of the user
* "following_count": Following count of the user
* "pp": Profile image URL of the user
* "downloaded": The date user was downloaded
* "demog_pred": Demographic predictions (gender, is_organization, age) of the user

### Tweets
Each tweet has:
* "_id": Tweet id from twitter
* "text": Text of the tweet
* "date": Date of the tweet
* "senti": Sentiment of the tweet (predicted by https://github.com/politusanalytics/twitter_sentiment_analysis/blob/a14355f3da8f20238892cae32417c7552df218a2/use_pretrained_model.py)
* "n_ent": Name entities of the tweet (https://github.com/politusanalytics/twitter_ner/blob/c180e1f285757502d1e3126a41a35a70b225c359/src/use_pretrained_model.py)
