from __future__ import absolute_import, print_function

import json, yaml

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from cassandra.cluster import Cluster



with open('twitter_keys.yaml','r') as f:
    twitter_keys = yaml.load(f)

node_ip = '52.8.70.34'
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
            INSERT INTO tweets (id, screen_name, text, timestamp, url)
            VALUES (?, ?, ?, ?, ?)
            """
        )
        self.counter = 0

    def on_data(self, data):
        decoded = json.loads(data)

        id          = int(decoded['id'])
        time_stamp  = int(decoded['timestamp_ms'])
        screen_name = str(decoded['user']['screen_name'])
        url         = str(decoded['user']['url'])
        text        = str(decoded['text'].encode('ascii', 'ignore'))
        #geo        = decoded['geo']

        parameters = [ id , screen_name, text, time_stamp, url ]
        session.execute(self.insert_statement, parameters)

        self.counter += 1
        print(self.counter)

        return True

def on_error(self, status):
    print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
    auth.set_access_token(twitter_keys['access_token'], twitter_keys['access_token_secret'])

    stream = Stream(auth, l)

    stream.filter(track=['python', 'java', 'ruby', 'javascript'])