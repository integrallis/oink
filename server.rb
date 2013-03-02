require 'sinatra'
require 'cql'
require 'active_support/all'
require 'simple_uuid'

set :server, 'thin' 

PUBLIC_USER = 'public'

#
# Root path returns the userline for the "public" user
#
get '/' do
  @oinks = get_oinks(PUBLIC_USER, 'timeline')
  erb :oinks
end   

#
# Get a User's Userline (a list of their oinks)
#
get '/oinker/:user_id/userline' do
  connect_cassandra
  @oinks = get_oinks(params[:user_id], 'userline')
  erb :oinks
end

#
# Get a User's Timeline (Oinks from the people you follow)
#
get '/oinker/:user_id/timeline' do
  connect_cassandra
  @oinks = get_oinks(params[:user_id], 'timeline')
  erb :oinks
end

#
# Post an oink - BAD: Yeah don't write data on a get, just for a quick test!
#
get '/oinker/:user_id/userline/oink/:body' do
  oink(params[:user_id], params[:body] )
  redirect "/oinker/#{params[:user_id]}/userline"
end   

#
# Follow somebody
#
get '/oinker/:user_id/friends/follow/:friend_id' do
  follow(params[:user_id], params[:friend_id] )
  redirect "/oinker/#{params[:user_id]}/friends"
end

#
# See your list of friends (people you follow)
#
get '/oinker/:user_id/friends' do
  @users = friends(params[:user_id])
  erb :oinkers
end

#
# See your list of followers
#
get '/oinker/:user_id/followers' do
  @users = followers(params[:user_id])
  erb :oinkers
end

private

def connect_cassandra
  @client = Cql::Client.new(keyspace: 'oink').start!
end  

def oink(user_id, body)
  connect_cassandra
  
  uuid = SimpleUUID::UUID.new
  guid = Cql::Uuid.new(uuid.to_guid)
  ts = uuid.to_time

  # insert the oink
  @client.execute("INSERT INTO oinks (oink_id, user_id, body) VALUES('#{guid}', '#{user_id}', '#{body}')")

  # insert the oink in the owner's userline
  @client.execute("INSERT INTO userline (oink_id, user_id, when) VALUES('#{guid}', '#{user_id}', '#{ts}')")

  # insert the oink into the public user timeline
  @client.execute("INSERT INTO timeline (oink_id, user_id, when) VALUES('#{guid}', '#{PUBLIC_USER}', '#{ts}')")

  # find all of the followers and the oink to their timeline
  rows = @client.execute("SELECT user_id, users FROM followers WHERE user_id = '#{user_id}'")
  unless rows.empty? 
    followers = rows.first["users"]
    followers.each do |follower|
      @client.execute("INSERT INTO timeline (oink_id, user_id, when) VALUES('#{guid}', '#{follower}', '#{ts}')")
    end
  end
end 

def get_oinks(user, line)
  connect_cassandra
  puts "SELECT oink_id FROM #{line} WHERE user_id = '#{user}' and when > '#{2.days.ago}'"
  rows = @client.execute("SELECT oink_id FROM #{line} WHERE user_id = '#{user}' and when > '#{2.days.ago}'")
  ids = rows.map { |r| r["oink_id"] }
  unless ids.empty? 
    @client.execute("SELECT oink_id, dateOf(oink_id), user_id, body FROM oinks WHERE oink_id IN (#{ids.map {|e|"'#{e}'"}.join(",")})")
  else
    []
  end
end

def follow(follower_id, followee_id)
  connect_cassandra
  @client.execute("UPDATE followers SET users = users + { '#{follower_id}' } WHERE user_id = '#{followee_id}'")
  @client.execute("UPDATE friends SET users = users + { '#{followee_id}' } WHERE user_id = '#{follower_id}'")
end   

def friends(user_id)
  connect_cassandra
  results = @client.execute("SELECT user_id, users FROM friends WHERE user_id = '#{user_id}'")
  results.empty? ? [] : results.first["users"]
end

def followers(user_id)
  connect_cassandra
  results = @client.execute("SELECT user_id, users FROM followers WHERE user_id = '#{user_id}'")
  results.empty? ? [] : results.first["users"]
end