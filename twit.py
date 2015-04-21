from __future__ import absolute_import, print_function

import json, yaml, traceback, logging

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from cassandra.cluster import Cluster

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
                logging.warning('Percent_full %s' % (decoded['warning']['percent_full']))

            elif 'limit' in decoded:
                pass
                #logging.info('Missed %s tweets' % (decoded['limit']['track']))

            elif 'id' in decoded:
                id = int(decoded['id'])
                text = str(decoded['text'].encode('ascii', 'ignore'))

                user_screen_name = str(decoded['user']['screen_name'])
                user_id          = int(decoded['user']['id'])
                user_url         = str(decoded['user']['url'])

                tweet_timestamp_ms = int(decoded['timestamp_ms'])
                parameters = [id, text, user_screen_name, user_id, user_url, tweet_timestamp_ms]

                self.session.execute(self.insert_statement, parameters)
                self.counter += 1

                if self.counter % 250 == 0 or self.counter == 1:
                    log_str = 'Tweet count: %s' % (self.counter)
                    logging.info(log_str)
                    print(log_str)

                return True
            else:
                logging.error('Unknown data format: %s' % (decoded))

        except Exception as err:
            logging.error(err)

    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    node_ip = '54.186.215.175'
    cluster = Cluster([node_ip])
    session = cluster.connect('twitter')

    with open('twitter_keys.yaml','r') as f:
        twitter_keys = yaml.load(f)

    logging.basicConfig(filename='example.log',level=logging.INFO,
        format='%(asctime)s: %(levelname)s %(name)s: %(message)s')

    listener = StdOutListener(session)
    auth     = OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
    auth.set_access_token(twitter_keys['access_token'], twitter_keys['access_token_secret'])

    stream = Stream(auth, listener)

    try:
        track = [
            'python',
            'ruby',
            'rails',
            'javascript',
            'java',
            'C',
            'C++',
            'iOS',
            'android'
        ]

        stream.filter(track=track, stall_warnings=True)
    except Exception as err:
        logging.critical(err)

    print('test')