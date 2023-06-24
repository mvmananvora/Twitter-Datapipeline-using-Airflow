import tweepy
import pandas as pd
import json 
import datetime as datetime
import s3fs

def run_twitter_etl():
    consumer_key = "xx"
    consumer_secret = "xx"
    access_key = "xx-xx"
    access_secret = "xx"

    # Twitter authentication
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)

    # Creating an api function
    api = tweepy.API(auth)

    tweets = api.user_timeline (screen_name='@elonmusk'
                                ,count=200,
                                include_rts=False,
                                tweet_mode='extended')

    print(tweets)


    list = []
    for tweet in tweets:
            text = tweet._json["full_text"]
            refined_tweet = {"user": tweet.user.screen_name,
                            'text' : text,
                            'favorite_count' : tweet.favorite_count,
                            'retweet_count' : tweet.retweet_count,
                            'created_at' : tweet.created_at}
            
            list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://manan-airflow-bucket/elone_tweets.csv')