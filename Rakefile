require 'rake'
require 'cql'

task :default => [:dbcreate]

task :dbcreate do
  keyspace_definition = File.open("cql/keyspace_definition.cql", "rb").read
  users_table_definition = File.open("cql/create_users.cql", "rb").read
  oinks_table_definition = File.open("cql/create_oinks.cql", "rb").read
  timeline_table_definition = File.open("cql/create_timeline.cql", "rb").read
  userline_table_definition = File.open("cql/create_userline.cql", "rb").read
  
  client = Cql::Client.connect(hosts: ['localhost'])
  
  client.execute(keyspace_definition)
  client.use('oink')
  client.execute(users_table_definition)
  client.execute(oinks_table_definition)
  client.execute(timeline_table_definition)
  client.execute(userline_table_definition)
end