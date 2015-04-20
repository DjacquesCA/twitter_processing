from __future__ import absolute_import, print_function

import json, yaml, time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from cassandra.cluster import Cluster

with open('twitter_keys.yaml','r') as f:
    twitter_keys = yaml.load(f)

node_ip = '52.11.49.19'
cluster = Cluster([node_ip])
session = cluster.connect('twitter')

def handle_insert_success(rows):
    print("Successful insert")
    print(rows)

def handle_insert_error(exception):
    log.error("Failed to insert tweet info: %s", exception)

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self):
        self.insert_statement = session.prepare(

            """
            INSERT INTO d_tweets (id, text, user_screen_name, user_id, user_url, tweet_timestamp_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        self.counter = 0

    def on_data(self, data):
        decoded = json.loads(data)

        id = int(decoded['id'])
        text = str(decoded['text'].encode('ascii', 'ignore'))

        user_screen_name = str(decoded['user']['screen_name'])
        user_id          = int(decoded['user']['id'])
        user_url         = str(decoded['user']['url'])

        tweet_timestamp_ms = int(decoded['timestamp_ms'])

        if 'warning' in decoded:
            print ("'Warning in decoded")
            if 'percent_full' in decoded['warning']:
                print ("Percent_full %s" % (decoded['warning']['percent_full']))
        #geo        = decoded['geo']

        parameters = [id, text, user_screen_name, user_id, user_url, tweet_timestamp_ms, curr_time_ms]

        session.execute(self.insert_statement, parameters)

        self.counter += 1

        print('Count: %s' % (self.counter))
        # print('Count: %s' % (self.counter))
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
    auth.set_access_token(twitter_keys['access_token'], twitter_keys['access_token_secret'])

    stream = Stream(auth, l)

    stream.filter(track=['basketball'], stall_warnings=True)