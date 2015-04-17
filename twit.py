from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json, yaml

from cassandra.cluster import Cluster

with open('twitter_keys.yaml','r') as f:
    twitter_keys = yaml.load(f)

cluster = Cluster(['52.8.41.30'])
session = cluster.connect('twitter')

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):

        decoded = json.loads(data)

        id = decoded['id']
        time_stamp = decoded['created_at']
        screen_name = decoded['user']['screen_name']
        url = decoded['user']['url']
        text = decoded['text'].encode('ascii', 'ignore')
        #geo = decoded['geo']

        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
    auth.set_access_token(twitter_keys['access_token'], twitter_keys['access_token_secret'])

    stream = Stream(auth, l)
    stream.filter(track=['basketball'])