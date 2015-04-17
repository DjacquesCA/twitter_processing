import ssl

from cassandra.cluster import Cluster

node_ip = '52.8.41.30'

cluster = Cluster([node_ip])

cluster.connect()