CREATE KEYSPACE oink WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};