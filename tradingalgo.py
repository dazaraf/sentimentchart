# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 17:55:19 2021

@author: Dudu Azaraf
"""
import requests
import json
from datetime import date
import pandas as pd
import time
import math
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import urllib 
import datetime
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
import plotly.graph_objects as go
from plotly.graph_objs import *
from plotly.subplots import make_subplots


analyzer = SentimentIntensityAnalyzer()

##read the sentiment daily chart
df_sentiment = pd.read_csv('sentimentdaily.csv')
df_sentiment = df_sentiment.iloc[:-1]

last_time = df_sentiment.iloc[-1]['time'] 
date_1 = datetime.datetime.strptime(last_time, '%Y-%m-%d')
start_from = date_1 + datetime.timedelta(days=1) + datetime.timedelta(hours=3)
start_timestamp = time.mktime(start_from.timetuple())
##NB! Instead of a 'while true', switch to a loop that ends when the timestamp gets to the current timestamp. Then wrap that in a while True loop that keeps trying the script

print("made it here1")
#Pushshift
main_url = "https://api.pushshift.io/reddit/search/submission/?subreddit=cryptocurrency&after=" + str(int(start_timestamp)) + "&sort=asc&size=500"
cryptocurrency_response = requests.get(main_url)
cryptocurrency_content = json.loads(cryptocurrency_response.content)    
cryptocurrency_content = cryptocurrency_content['data']
df = pd.DataFrame(cryptocurrency_content)
df = df.replace(np.nan, '', regex=True)
df['fulltext'] =  df['title'] + '\n' + df['selftext']
df = df[~(df['fulltext'].str.contains('[removed]', regex = False, na = True))]
df = df[~(df['fulltext'].str.contains('[deleted]', regex = False, na = True))]
df_short = df[['id', 'created_utc', 'title' ,'selftext', 'fulltext']]
df_short['polarity_fulltext_pos'] = df_short['fulltext'].apply(lambda x: analyzer.polarity_scores(x)['pos'])
df_short['polarity_fulltext_neg'] = df_short['fulltext'].apply(lambda x: analyzer.polarity_scores(x)['neg'])
df_short['polarity_fulltext_neu'] = df_short['fulltext'].apply(lambda x: analyzer.polarity_scores(x)['neu']) 
df_short['dudu_score'] = (df_short['polarity_fulltext_pos']-df_short['polarity_fulltext_neg'])/df_short['polarity_fulltext_neu']
timestamp = cryptocurrency_content[-1]['created_utc']

current_timestamp = time.time()
while timestamp < current_timestamp:
        try:
            print ("scraping from timestamp")
            print(pd.to_datetime(timestamp, unit = 's'))
            timestamp_string = str(timestamp)
            main = "https://api.pushshift.io/reddit/search/submission/?subreddit=cryptocurrency&after="+timestamp_string+"&sort=asc&size=500"
            cryptocurrency_response_main = requests.get(main)
            cryptocurrency_content_main = json.loads(cryptocurrency_response_main.content)
            cryptocurrency_content_main2 = cryptocurrency_content_main['data']
            df_main = pd.DataFrame(cryptocurrency_content_main2)
            df_main = df_main.replace(np.nan, '', regex=True)
            df_main['fulltext'] =  df_main['title'] + '\n' + df_main['selftext']
            df_main = df_main[~(df_main['fulltext'].str.contains('[removed]', regex = False, na = True))]
            df_main = df_main[~(df_main['fulltext'].str.contains('[deleted]', regex = False, na = True))]
            df_main = df_main[['id', 'created_utc', 'title' ,'selftext', 'fulltext']]
            df_main['polarity_fulltext_pos'] = df_main['fulltext'].apply(lambda x: analyzer.polarity_scores(x)['pos'])
            df_main['polarity_fulltext_neg'] = df_main['fulltext'].apply(lambda x: analyzer.polarity_scores(x)['neg'])
            df_main['polarity_fulltext_neu'] = df_main['fulltext'].apply(lambda x: analyzer.polarity_scores(x)['neu'])  
            df_main['dudu_score'] = (df_main['polarity_fulltext_pos']-df_main['polarity_fulltext_neg'])/df_main['polarity_fulltext_neu']
            df_short = pd.concat([df_short,df_main])
            print("ended on timestamp")
            timestamp = cryptocurrency_content_main2[-1]['created_utc']
            timestamp = int(timestamp)
            print (timestamp)
            time.sleep(1)
        except:
            print('Oh No! quick 30 second break and then trying again')
            time.sleep(30)
            pass
        
criteria = (df_short['polarity_fulltext_neu'] == 0) | ((df_short['polarity_fulltext_neg'] == 0) & (df_short['polarity_fulltext_pos'] == 0))
df_agg = df_short[~criteria]
df_agg['polarity_fulltext_neg'] = df_agg['polarity_fulltext_neg'] + 0.0001
df_agg['dudu_score'] = (df_agg['polarity_fulltext_pos']-df_agg['polarity_fulltext_neg'])/df_agg['polarity_fulltext_neu']
df_agg['time'] = pd.to_datetime(df_agg['created_utc'], unit = 's')
df_agg = df_agg.dropna()
df_agg['time'] = df_agg['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
df_agg = df_agg.groupby('time').mean()
df_agg = df_agg['dudu_score'].reset_index()


df_max = pd.concat([df_sentiment, df_agg])
df_max.to_csv('sentimentdaily.csv')



##Combine the sentiment with the price data
url = "https://min-api.cryptocompare.com/data/v2/histoday?fsym=BTC&tsym=USD&limit=2000&api_key=d1875a3943f6f2ee83a90ac2e05d5fa018618e00724e9018f9bd08c0ac932cc6"
btc = json.loads(urllib.request.urlopen(url).read())
btc_price = btc['Data']['Data']
btc_df = pd.DataFrame(btc_price)
btc_df['time'] = pd.to_datetime(btc_df['time'], unit = 's')
btc_df['time'] = btc_df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
btc_df2 = btc_df[['time', 'close']]
btc_df2.columns = ['time', 'price']
btc_df2 = btc_df2.set_index('time')
df_max = df_max.set_index('time')
joint = df_max.join(btc_df2)
joint_decision = joint


##plot the sentiment data
sentiment_fig = make_subplots(specs=[[{"secondary_y": True}]])
sentiment_fig.add_trace(
                go.Scatter(name='Sentiment', x=joint.index, y=joint['dudu_score'])
            )
sentiment_fig.add_trace(
                go.Scatter(name='Price', x=joint.index, y=joint['price']), secondary_y = True
    )
sentiment_fig.update_layout(title = 'Reddit Sentiment(blue) vs Bitcoin Price(red)', titlefont_size=32)

plot(sentiment_fig)