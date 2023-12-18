import tweepy
import pandas as pandas
import json
from datetime import datetime
import s3fs


def run_twitter_etl():

    access_key = "3XB12MSfz9E9xPTRFPCYS3PgQ"
    access_secret = "kylzSn3tlDTg4Jiw5g3QhUu1w9JPghR1ZDIPcUmUuwEYH8nKEJ"
    consumer_key = "1736805951930249216-IpbPMqzAdRnvXTNwW4tddbu2UC73fN"
    consumer_secret = "oGAHTyU77aFmAex5SToIjJsLS6dn49sH4Ugn6C4O0SeYu"

    #Twitter Authentication
    #connection between code and twitter API

    auth=tweepy.OAuthHandler(access_key,access_secret)
    auth.set_access_token(consumer_key,consumer_secret)


    #creating an API object

    api = tweepy.API(auth) # you can function inside the tweepy

    tweets=api.user_timeline(screen_name='@kattyperry',
                            #Give the count that you need 200 is max how many tweets you need from that timeline
                            count=200,
                            include_rts=False, #if we wnt to abstract narendra modi's retweet we can do that by doing it true

                            tweet_mode='extended'

                            )


    tweet_list=[]
    for tweet in tweets:
        text=tweet.json["full text"]

        refined_tweet={
            "user": tweet.user.screen_name,
            'text':text,
            'favorite_count':tweet.favorite_count,
            'retweet_count':tweet.retweet_count,
            'created_at':tweet.created_at
        }

    tweet_list.append(refined_tweet)


    df=pd.DataFrame(tweet_list)
    df.to_csv("s3://ssakshi697-airflow-bucket/tweets.csv")