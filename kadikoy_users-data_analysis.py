from datetime import datetime
import os
import sys
import pandas as pd
from nltk.tokenize import TweetTokenizer
from nltk.util import ngrams
import re
import string
import matplotlib.pyplot as plt
from collections import Counter
from unicode_tr import unicode_tr
from unicode_tr.extras import slugify
import pymongo
from common import generic_get_users, generic_get_tweets

today = datetime.now().strftime("%Y%m%d")
remove_punc = [item for item in string.punctuation] + ['…', '’', '...', '..', ':)', '“', '”']

# Create directories to export figures
if "figs" not in os.listdir():
    os.mkdir("figs")
if "pdf" not in os.listdir("figs"):
    os.mkdir("figs/pdf")
if "png" not in os.listdir("figs"):
    os.mkdir("figs/png")
if today not in os.listdir(f"figs/pdf"):
    os.mkdir(f"figs/pdf/{today}")
if today not in os.listdir(f"figs/png"):
    os.mkdir(f"figs/png/{today}")

# Get year and months to filter tweets
start_date = sys.argv[1]
end_date = sys.argv[2]
# Get stop words
stop_words_path = sys.argv[3]
with open(stop_words_path, "r", encoding="utf-8") as f:
    stop_words = f.read().split(",")
# Get users to be removed (spamming users)
remove_user_ids_path = sys.argv[4]
with open(remove_user_ids_path, "r") as f:
    remove_user_ids = [str(user_id) for user_id in f.read().split(",")]

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["politus_twitter"]
# Get the collections(tables)
user_col = db["users"]
tweet_col = db["tweets"]

# Get tweets
tweet_ids = []
users = generic_get_users(kadikoy=True, tweet_date=[start_date, end_date], return_only_filter_element=True, columns_to_return=["tweets"])
for user in users:
    if user["_id"] not in remove_user_ids:
        for tweet in user["tweets"]:
            if tweet["type"] in ["original", "retweet", "fav"]:
                tweet_ids.append(tweet["id"])
            elif tweet["type"] in ["reply", "quote"]:
                tweet_ids.append(tweet["id"])
                tweet_ids.append(tweet["ref_id"])
tweets = generic_get_tweets(ids=tweet_ids, columns_to_return=["text"])
tweet_texts = [tweet["text"] for tweet in tweets]

# Preprocessing
def remove_emojis(text):
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', text)

def preprocess_tweets(tweet_texts):
    tt = TweetTokenizer(preserve_case=True)
    tweet_texts_processed = []
    
    for tweet_text in tweet_texts:
        # Remove emojis
        tweet_text = remove_emojis(tweet_text)
        # Remove Foursquare updates
        if "w/" in tweet_text and "others" in tweet_text:
            continue
        # Remove YouTube updates
        if ("via" in tweet_text and "@YouTube" in tweet_text) or ("aracılığıyla" in tweet_text and "@YouTube" in tweet_text):
            continue
        # Remove Change.org updates
        if ("via" in tweet_text and "@ChangeTR" in tweet_text) or ("aracılığıyla" in tweet_text and "@ChangeTR" in tweet_text):
            continue
        # Remove numbers
        #tweet_text = ''.join([i for i in tweet_text if not i.isdigit()])
        # Remove URLs
        tweet_text = re.sub(r'http\S+', '', tweet_text)
        # Lowercasing
        tweet_text = unicode_tr(tweet_text).lower()
        # Tokenization
        tweet_text = [token for token in tt.tokenize(tweet_text) if token not in stop_words and token not in remove_punc]
        # Slugify hashtags
        tweet_text = ["#" + slugify(token) if token[0] == "#" else token for token in tweet_text]
        tweet_texts_processed.append(tweet_text)
    return tweet_texts_processed

# Hashtag Counter
remove_hashtags = ["#nowplaying", "#nowatching"]
def hashtag_counter(tweet_texts):
    """
    Input: Tokenized tweet texts
    Output: Counts of most common 100 hashtags
    """
    hashtags = []
    for tweet_text in tweet_texts:
        for token in tweet_text:
            if token[0] == "#" and len(token) > 1 and token not in remove_hashtags:
                hashtags.append(token)
    hashtag_counts = pd.concat([pd.Series([item[0] for item in Counter(hashtags).most_common(100)], name="hashtag"), pd.Series([item[1] for item in Counter(hashtags).most_common(100)], name="count")], axis=1)
    return hashtag_counts

# Mention Counter
def mention_counter(tweet_texts):
    """
    Input: Tokenized tweet texts
    Output: Counts of most common 100 mentions
    """
    mentions = []    
    for tweet_text in tweet_texts:
        for token in tweet_text:
            if token[0] == "@" and len(token) > 1:
                mentions.append(token)
    mention_counts = pd.concat([pd.Series([item[0] for item in Counter(mentions).most_common(100)], name="mention"), pd.Series([item[1] for item in Counter(mentions).most_common(100)], name="count")], axis=1)
    return mention_counts

# N-Gram Counts
def get_ngrams(tweet_texts, n):
    """
    Input: Tokenized tweet texts, n-gram
    Output: Counts of n-grams
    """
    ngram_tokens = []
    for tweet_text in tweet_texts:
        ngram_tokens.extend(ngrams(tweet_text, n=n))
    ngram_counts = Counter(ngram_tokens).most_common()
    ngram_counts = pd.DataFrame(ngram_counts).rename({0:"word", 1:"count"}, axis=1)
    ngram_counts["word"] = ngram_counts["word"].apply(lambda x: ' '.join(x))
    return ngram_counts

# Visualizations
## Hashtags
def visualize_hashtags(tweet_texts, start_date, end_date):
    plt.figure(figsize=(14, 14))
    hashtag_counts = hashtag_counter(tweet_texts)
    plt.barh(hashtag_counts.loc[:50, "hashtag"], hashtag_counts.loc[:50, "count"])
    plt.yticks(fontsize=12)

    plt.title(f"Hashtag Counts ({start_date} - {end_date})", fontsize=16)
    plt.savefig(f"figs/pdf/{today}/hashtags_{start_date}_{end_date}.pdf", format="pdf", bbox_inches='tight')
    plt.savefig(f"figs/png/{today}/hashtags_{start_date}_{end_date}.png", format="png", bbox_inches='tight')
    plt.close()

## Mentions
def visualize_mentions(tweet_texts, start_date, end_date):
    plt.figure(figsize=(14, 14))
    mention_counts = mention_counter(tweet_texts)
    plt.barh(mention_counts.loc[:50, "mention"], mention_counts.loc[:50, "count"])
    plt.yticks(fontsize=12)
    plt.title(f"Mention Counts ({start_date} - {end_date})", fontsize=16)
    plt.savefig(f"figs/pdf/{today}/mentions_{start_date}_{end_date}.pdf", format="pdf", bbox_inches='tight')
    plt.savefig(f"figs/png/{today}/mentions_{start_date}_{end_date}.png", format="png", bbox_inches='tight')
    plt.close()

## N-Grams
def visualize_ngrams(unigram_counts, bigram_counts, trigram_counts, quadrigram_counts, start_date, end_date):
    plt.figure(figsize=(30, 30), dpi=200)
    
    plt.subplot(2, 2, 1)
    plt.barh(unigram_counts.loc[:50, "word"], unigram_counts.loc[:50, "count"])
    plt.yticks(fontsize=16)
    plt.title(f"Unigram Counts ({start_date} - {end_date})", fontsize=24)
    
    plt.subplot(2, 2, 2)
    plt.barh(bigram_counts.loc[:50, "word"], bigram_counts.loc[:50, "count"])
    plt.yticks(fontsize=16)
    plt.title(f"Bigram Counts ({start_date} - {end_date})", fontsize=24)
        
    plt.subplot(2, 2, 3)
    plt.barh(trigram_counts.loc[:50, "word"], trigram_counts.loc[:50, "count"])
    plt.yticks(fontsize=16)
    plt.title(f"Trigram Counts ({start_date} - {end_date})", fontsize=24)
    
    plt.subplot(2, 2, 4)
    plt.barh(quadrigram_counts.loc[:50, "word"], quadrigram_counts.loc[:50, "count"])
    plt.yticks(fontsize=16)
    plt.title(f"Quadrigram Counts ({start_date} - {end_date})", fontsize=24)

    plt.tight_layout()
    
    plt.savefig(f"figs/pdf/{today}/ngrams_{start_date}_{end_date}.pdf", format="pdf", bbox_inches="tight")
    plt.savefig(f"figs/png/{today}/ngrams_{start_date}_{end_date}.png", format="png", bbox_inches="tight")
    
    plt.close()


tweet_texts_processed = preprocess_tweets(tweet_texts)

visualize_hashtags(tweet_texts_processed, start_date, end_date)
visualize_mentions(tweet_texts_processed, start_date, end_date)

unigram_counts = get_ngrams(tweet_texts_processed, n=1)
bigram_counts = get_ngrams(tweet_texts_processed, n=2)
trigram_counts = get_ngrams(tweet_texts_processed, n=3)
quadrigram_counts = get_ngrams(tweet_texts_processed, n=4)
visualize_ngrams(unigram_counts, bigram_counts, trigram_counts, quadrigram_counts, start_date, end_date)