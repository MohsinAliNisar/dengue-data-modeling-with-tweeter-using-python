# encoding: utf-8
from tweepy.streaming import StreamListener
from _csv import writer
import csv
from tweepy import OAuthHandler
from tweepy import Stream
import json
import csv
import re
import psycopg2
#Twitter API credentials
consumer_key = "yYkNPCfWkQbCOgnIubXQaOuJd"
consumer_secret = "DU1As8mDTp8b7VaSew4cd6zdvDDM7ftfi4zBdw658yf0TsOzVG"
access_key = "454993835-ckDfmtND8gcYOJXCewWKFm8sOa8zAqeyk17evZPI"
access_secret = "KtTJ4S5XMeRuiVlLxNYpbnUKclDwDRSBGdsPfhLlBhbf4"


#Import the necessary methods from tweepy library



#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        # Open/create a file to append data to
        #Use csv writer
        tweet = json.loads(data)  
        conn = psycopg2.connect("host='localhost' dbname='postgres' user='postgres' password='perform.123'")
        print('saving...')
        cursor = conn.cursor()
        query =  "INSERT INTO tweets (tweet_data_json, tweet_data_jsonb, tweet_text, tweet_id, screen_name, created_at) VALUES (%s, %s,%s, %s,%s,%s);"
        values = (json.dumps(tweet), json.dumps(tweet),tweet['text'],tweet['user']['id'],tweet['user']['screen_name'],tweet['user']['created_at'])
        cursor.execute(query, values)
        conn.commit()
        print('saved')
        print('@%s: %s' % (tweet['user']['screen_name'], tweet['text'].encode('ascii', 'ignore')))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter
    #Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    stream = Stream(auth, l)
    loc = [62.9,25.2,73.3,35.2] #Pakistan

    #stream.filter(follow=['372335990'])
    #stream.filter(track=['@FightDenguePak'],locations = loc)
    stream.filter(follow=['372335990','1513673754'],track=['FightDenguePak','dengue','dengue fever','Dengue Fever','KPK','Peshawar', 'Karachie','Lahore', 'ڈینگی بخار'],locations = loc)
    