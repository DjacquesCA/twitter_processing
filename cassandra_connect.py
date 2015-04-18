import ssl

from cassandra.cluster import Cluster

node_ip = '52.8.70.34'

cluster = Cluster([node_ip])

session = cluster.connect('twitter')

result = session.execute(
    """
    INSERT INTO tweets (id, screen_name, text, timestamp, url)
    VALUES (%(id)s, %(screen_name)s, %(text)s, %(timestamp)s, %(url)s)
    """,
    {
        'id': 30,
        'screen_name': 'darren',
        'text': 'darren\'s tweet',
        'timestamp': 54523213,
        'url': 'http://darren.com'
    }
)

print(result)

