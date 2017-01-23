#! /usr/bin/python
''' The script is used to pull data from twitter using twython and dumping the data into elasticsearch'''
import config as Config #Using config file to read the config settings from a separate config file.
from twython import Twython
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import *

#es = Elasticsearch([Config.ES])
es= Elasticsearch()
TWITTER_APP_KEY = Config.TWITTER_APP_KEY
TWITTER_APP_KEY_SECRET = Config.TWITTER_APP_KEY_SECRET
TWITTER_ACCESS_TOKEN = Config.TWITTER_ACCESS_TOKEN
TWITTER_ACCESS_TOKEN_SECRET = Config.TWITTER_ACCESS_TOKEN_SECRET

twitterauth = Twython(app_key=TWITTER_APP_KEY,
            app_secret=TWITTER_APP_KEY_SECRET,
            oauth_token=TWITTER_ACCESS_TOKEN,
            oauth_token_secret=TWITTER_ACCESS_TOKEN_SECRET)

try:
    es.indices.create(index='twitter', ignore=400)
    mapping = {"twitter": { "properties": { "geo": {"properties": { "coordinates": { "type": "geo_point" }, "type": {"type": "string"}}}}}}
    #mapping = {"twitter": { "properties": { "coordinates.coordinates": { "type": "geo_point" }, "type": {"type": "string"}}}}
    es.indices.put_mapping(index='twitter',doc_type='twitter',body=mapping)
except Exception, e:
    pass

def pull_tweets(keyword):
    search = twitterauth.search(q=keyword,count=100)
    tweets = []
    tweets = search['statuses']
    for tweet in tweets:
        if tweet['geo'] != None:
            #print tweet
            #tweet['geo']['coordinates']=json.load(tweet['geo']['coordinates'])
            #tweet['geo']['type']='geo_point'
            '''doc = {
                'text': tweet['text'],
                'geo': tweet['geo']['coordinates'],
                'time': datetime.now()
            }'''
            if tweet['geo']['coordinates'][1]<=-85 or tweet['geo']['coordinates'][1]>=85:
                continue

            res = es.index(index="twitter",doc_type='twitter', body=tweet)
            print(res['created'])



def twittmap():
    try:
        for i in range(1,50):
            pull_tweets('java')
            pull_tweets('love')
            pull_tweets('trump')
            pull_tweets('clinton')
            pull_tweets('india')
            pull_tweets('diwali')
            pull_tweets('movie')
            pull_tweets('music')
    except:
	#pass
	return

twittmap()
pull_tweets
