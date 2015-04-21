from __future__ import absolute_import, print_function

import json, yaml, traceback

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from cassandra.cluster import Cluster

with open('twitter_keys.yaml','r') as f:
    twitter_keys = yaml.load(f)


class StdOutListener(StreamListener):
    """ A listener handles tweets that are the received from the stream.
    This listener inserts tweet data into a given Cassandra cluster.
    """
    def __init__(self, session):
        self.counter = 0
        self.session = session
        self.insert_statement = self.session.prepare(

            """
            INSERT INTO d_tweets (id, text, user_screen_name, user_id, user_url, tweet_timestamp_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )

    def on_data(self, data):
        try:
            decoded = json.loads(data)

            if 'warning' in decoded:
                print ("Percent_full %s" % (decoded['warning']['percent_full']))

            else:
                id = int(decoded['id'])
                text = str(decoded['text'].encode('ascii', 'ignore'))

                user_screen_name = str(decoded['user']['screen_name'])
                user_id          = int(decoded['user']['id'])
                user_url         = str(decoded['user']['url'])

                tweet_timestamp_ms = int(decoded['timestamp_ms'])
                #geo        = decoded['geo']

                parameters = [id, text, user_screen_name, user_id, user_url, tweet_timestamp_ms]

                self.session.execute(self.insert_statement, parameters)

                self.counter += 1

                print('Count: %s' % (self.counter))
                return True

        except:
            traceback.print_exc()

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    node_ip = '52.11.49.19'
    cluster = Cluster([node_ip])
    session = cluster.connect('twitter')

    listener = StdOutListener(session)
    auth     = OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
    auth.set_access_token(twitter_keys['access_token'], twitter_keys['access_token_secret'])

    stream = Stream(auth, listener)

    try:
        stream.filter(track=['basketball'], stall_warnings=True)
        except:
            traceback.print_exc()