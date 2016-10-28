import certifi
import sys
import os
import json
import time
import config as Config
from flask import Flask
from pprint import pprint
from elasticsearch import Elasticsearch
from flask import Flask, request, render_template, g, redirect, Response, make_response, jsonify

application = Flask(__name__)  
app = application

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/search/<keyword>', methods=['GET'])
def search(keyword):

    try:
        #es = Elasticsearch([Config.ES])
        es= Elasticsearch()
        TWITTER_APP_KEY = Config.TWITTER_APP_KEY
        TWITTER_APP_KEY_SECRET = Config.TWITTER_APP_KEY_SECRET
        TWITTER_ACCESS_TOKEN = Config.TWITTER_ACCESS_TOKEN
        TWITTER_ACCESS_TOKEN_SECRET = Config.TWITTER_ACCESS_TOKEN_SECRET

        res = es.search(index="twitter",scroll='1s',search_type='scan',size=10000, body={"query": {"match": { "text": { "query": keyword, "operator": "or" } } } })
        output=[]
        tweets=[]
        scroll_size=res["hits"]["total"]
        #output.append(scroll_size)
        while scroll_size>0:
            scroll_id=res['_scroll_id']
            rs=es.scroll(scroll_id=scroll_id,scroll='100s')
            tweets+=rs['hits']['hits']
            scroll_size=len(res['hits']['hits'])
        for doc in tweets:
                output.append(doc['_source']['geo']['coordinates'])
        return Response(json.dumps(output), content_type='application/json')
        
    except Exception as e:
        pass

@app.route('/searchLocal/<keyword>', methods=['GET'])
def searchLocal(keyword):
    #keyword=request.args.get('keyword')
    lat=request.args.get('lat')
    lon=request.args.get('lon')
    try:
        lat, lon = float(lat), float(lon)
    except ValueError, e:
        #return jsonify({"results": []})
        return Response(json.dumps(), content_type='application/json')
    if lat == None or lon == None:
        return Response(json.dumps(), content_type='application/json')

    try:
        #es = Elasticsearch([Config.ES])
        es= Elasticsearch()
        TWITTER_APP_KEY = Config.TWITTER_APP_KEY
        TWITTER_APP_KEY_SECRET = Config.TWITTER_APP_KEY_SECRET
        TWITTER_ACCESS_TOKEN = Config.TWITTER_ACCESS_TOKEN
        TWITTER_ACCESS_TOKEN_SECRET = Config.TWITTER_ACCESS_TOKEN_SECRET
        #,"match": { "text": { "query": keyword, "operator": "or" } }
        #queryBody= {"query":{"filtered":{"filter" : {"geo_distance" : {"distance" : "2000km","geo.coordinates" : [ lat,lon]}}} }}
        #queryBody= {"query":{"filtered":{"query":{"match_all":{}},"filter" : {"geo_distance" : {"distance" : "2000km","geo.coordinates" : [lat,lon]}}}}}
        queryBody= {"query":{"filtered":{"query":{"match":{"text": { "query": keyword, "operator": "or" }}},"filter" : {"geo_distance" : {"distance" : "500km","geo.coordinates" : [lat,lon]}}}}}
        #queryBody= {"query":{"filtered":{"filter" : {"geo_distance" : {"distance" : "2000km","geo.coordinates" : [ lat,lon]}}}}}

        res = es.search(index="twitter",scroll='1s',search_type='scan',size=10000, body=queryBody)
        #return Response(json.dumps(res), content_type='application/json')

        output=[]
        tweets=[]
        scroll_size=res["hits"]["total"]
        #output.append(scroll_size)
        #return Response(json.dumps(scroll_size), content_type='application/json')

        while scroll_size>0:
            scroll_id=res['_scroll_id']
            rs=es.scroll(scroll_id=scroll_id,scroll='100s')
            tweets+=rs['hits']['hits']
            scroll_size=len(res['hits']['hits'])

        #print tweets
        #return Response(json.dumps(), content_type = "application/json")
        #return Response(json.dumps(tweets), content_type='application/json')

        for doc in tweets:
            output.append(doc['_source']['geo']['coordinates'])
        return Response(json.dumps(output), content_type='application/json')

    except Exception as e:
        pass

if __name__ == "__main__":         
        app.run(threaded=True)