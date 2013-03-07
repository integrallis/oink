require 'sinatra'
require 'cql'
require 'active_support/all'
require 'simple_uuid'
require 'sinatra/flash'
require 'bcrypt'
require 'hashie'

set :server, 'thin' 
enable :sessions
set :session_secret, 'BaCoNYum!'
set :erb, :layout => :layout 

PUBLIC_USER = 'public'

['/logout', '/oinker/*', '/oinkers' ].each do |path|
  before path do
    authenticate!
  end
end

# -----------------------------------------------------------------------------
# Root path returns the timeline for the currently logged in user or 
# for the "public" user if not logged in
# -----------------------------------------------------------------------------
get '/' do
  user_id = session[:user_id]
  @user = get_user(user_id) if user_id
  if @user.nil? 
    @oinks = get_oinks(PUBLIC_USER, 'timeline')
    erb :oinks
  else 
    redirect "/oinker/#{@user.user_id}/timeline"
  end
end   

# -----------------------------------------------------------------------------
# Login
# -----------------------------------------------------------------------------
post '/login' do
  session[:user_id] = nil # you try to login I'll log you out swine!
  
  user_id = params[:user_id]
  password = params[:password]
  
  saved_password = get_password_for(user_id)

  if !password.blank? && saved_password == password
    session[:user_id] = user_id
    flash[:success] = "You have been successfully logged in my little piggy!"
  else
    flash[:error] = "You Bacon is Not Up To Snuff, Try Again Little Piggie!"
  end
  
  redirect "/"
end

# -----------------------------------------------------------------------------
# Logout
# -----------------------------------------------------------------------------
delete '/logout' do
  session[:user_id] = nil
  
  redirect "/"
end

# -----------------------------------------------------------------------------
# Register
# -----------------------------------------------------------------------------
post '/register' do
  session[:user_id] = nil # you try to login I'll log you out swine!

  user_id = params[:user_id]
  username = params[:username]

  unless exists?(user_id)
    password = BCrypt::Password.create(params[:password])
    @client.execute("INSERT INTO users (user_id, name, password) VALUES('#{user_id}', '#{username}', '#{password}')")
    flash[:success] = "You have been successfully register my little piggy!"
  else
    flash[:error] = "Oh you poor swine, have you forgotten you've already registered?"
  end
  
  redirect "/"
end

# -----------------------------------------------------------------------------
# Get a User's Userline (a list of their oinks)
# -----------------------------------------------------------------------------
get '/oinker/:user_id/userline' do
  @oinks = get_oinks(@user.user_id, 'userline')
  erb :oinks
end

# -----------------------------------------------------------------------------
# Get a User's Timeline (Oinks from the people you follow)
# -----------------------------------------------------------------------------
get '/oinker/:user_id/timeline' do
  @oinks = get_oinks(@user.user_id, 'timeline')
  erb :oinks
end

# -----------------------------------------------------------------------------
# Post an oink
# -----------------------------------------------------------------------------
post '/oinker/:user_id/userline/oink' do
  oink(params[:user_id], params[:body] )
  redirect "/oinker/#{params[:user_id]}/userline"
end   

# -----------------------------------------------------------------------------
# Get list of all Oinkers in the system
# -----------------------------------------------------------------------------
get '/oinkers' do
  @users = get_users
  erb :oinkers
end

# -----------------------------------------------------------------------------
# See your list of friends (people you follow)
# -----------------------------------------------------------------------------
get '/oinker/:user_id/friends' do
  @users = friends(@user.user_id)
  erb :oinkers
end

# -----------------------------------------------------------------------------
# See your list of followers
# -----------------------------------------------------------------------------
get '/oinker/:user_id/followers' do
  @users = followers(@user.user_id)
  erb :oinkers
end

# -----------------------------------------------------------------------------
# Follow somebody
# -----------------------------------------------------------------------------
post '/oinker/follow' do
  follow(params[:user_id], params[:friend_id] )
  redirect "/oinker/#{params[:user_id]}/friends"
end

# =============================================================================
private
# =============================================================================

# -----------------------------------------------------------------------------
# Connect to the cassandra client - have to do this on each request
# -----------------------------------------------------------------------------
def connect_cassandra
  @client = Cql::Client.new(keyspace: 'oink').start!
end 

# -----------------------------------------------------------------------------
# Authenticate
# -----------------------------------------------------------------------------
def authenticate!
  if session[:user_id].nil?
    flash[:error] = "That'll do pig! How about you log in first!"
    redirect "/"
  end
  connect_cassandra
  user_id = session[:user_id]
  @user = get_user(user_id)
end

# -----------------------------------------------------------------------------
# Retrieve password column for user
# -----------------------------------------------------------------------------
def get_password_for(user_id)
  connect_cassandra
  rows = @client.execute("SELECT password FROM users WHERE user_id = '#{user_id}'")
  rows.empty? ? nil : BCrypt::Password.new(rows.first["password"])
end 

# -----------------------------------------------------------------------------
# Check if user exist in the system
# -----------------------------------------------------------------------------
def exists?(user_id)
  connect_cassandra
  rows = @client.execute("SELECT user_id FROM users WHERE user_id = '#{user_id}'")
  !rows.empty? && rows.size == 1
end

# -----------------------------------------------------------------------------
# Retrieve user name, counts for oinks, followers and friends
# -----------------------------------------------------------------------------
def get_user(user_id)
  connect_cassandra
  
  user_row = @client.execute("SELECT user_id, name FROM users WHERE user_id = '#{user_id}'").first
  oinks_count = @client.execute("SELECT count(*) FROM oinks WHERE user_id = '#{user_id}' ALLOW FILTERING").first
  followers_row = @client.execute("SELECT users FROM followers WHERE user_id = '#{user_id}'").first
  friends_row = @client.execute("SELECT users FROM friends WHERE user_id = '#{user_id}'").first
  
  followers_count = followers_row.nil? ? 0 : followers_row["users"].size
  friends_count = friends_row.nil? ? 0 : friends_row["users"].size
  
  user = Hashie::Mash.new
  user.user_id = user_row["user_id"]
  user.name = user_row["name"]
  user.oinks = oinks_count["count"]
  user.followers = followers_count
  user.friends = friends_count
  user
end

# -----------------------------------------------------------------------------
# Get all oinkers in the system 
# This wouldn't scale in a real big data scenario and would have to be 
# replace with a smart search feature (find interesting people to follow)
# -----------------------------------------------------------------------------
def get_users
  connect_cassandra
  
  users = []
  user_rows = @client.execute("SELECT user_id, name FROM users")
  user_rows.each do |user_row| 
    user = Hashie::Mash.new
    user.user_id = user_row["user_id"]
    user.name = user_row["name"]

    users << user
  end
  
  users
end

# -----------------------------------------------------------------------------
# Return the list of friends for a user
# -----------------------------------------------------------------------------
def friends(user_id)
  connect_cassandra
  results = @client.execute("SELECT user_id, users FROM friends WHERE user_id = '#{user_id}'")
  user_ids = results.empty? ? [] : results.first["users"]
  
  users = []
  user_ids.each do |user_id| 
    user = Hashie::Mash.new
    user.user_id = user_id

    users << user
  end
  
  users
end

# -----------------------------------------------------------------------------
# Return the list of follower for a user
# -----------------------------------------------------------------------------
def followers(user_id)
  connect_cassandra
  results = @client.execute("SELECT user_id, users FROM followers WHERE user_id = '#{user_id}'")
  user_ids = results.empty? ? [] : results.first["users"]
  
  users = []
  user_ids.each do |user_id| 
    user = Hashie::Mash.new
    user.user_id = user_id

    users << user
  end
  
  users  
end

# -----------------------------------------------------------------------------
# Post an Oink!
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Get a list of Oink Ids from a user's userline or timeline 
# (line parameter is 'userline' or 'timeline') 
# -----------------------------------------------------------------------------
def get_oinks(user, line)
  connect_cassandra
  puts "SELECT oink_id FROM #{line} WHERE user_id = '#{user}' and when > '#{30.days.ago}'"
  rows = @client.execute("SELECT oink_id FROM #{line} WHERE user_id = '#{user}' and when > '#{2.days.ago}'")
  ids = rows.map { |r| r["oink_id"] }
  unless ids.empty? 
    @client.execute("SELECT oink_id, dateOf(oink_id), user_id, body FROM oinks WHERE oink_id IN (#{ids.map {|e|"'#{e}'"}.join(",")})")
  else
    []
  end
end

# -----------------------------------------------------------------------------
# Follow a user (makes them your friend)
# -----------------------------------------------------------------------------
def follow(follower_id, followee_id)
  connect_cassandra
  @client.execute("UPDATE followers SET users = users + { '#{follower_id}' } WHERE user_id = '#{followee_id}'")
  @client.execute("UPDATE friends SET users = users + { '#{followee_id}' } WHERE user_id = '#{follower_id}'")
end   

# -----------------------------------------------------------------------------
# Utility method to return relative time in words
# -----------------------------------------------------------------------------
def relative_time_ago(from_time)
  distance_in_minutes = (((Time.now - from_time.to_time).abs)/60).round
  case distance_in_minutes
    when 0..1 then 'about a minute'
    when 2..44 then "#{distance_in_minutes} minutes"
    when 45..89 then 'about 1 hour'
    when 90..1439 then "about #{(distance_in_minutes.to_f / 60.0).round} hours"
    when 1440..2439 then '1 day'
    when 2440..2879 then 'about 2 days'
    when 2880..43199 then "#{(distance_in_minutes / 1440).round} days"
    when 43200..86399 then 'about 1 month'
    when 86400..525599 then "#{(distance_in_minutes / 43200).round} months"
    when 525600..1051199 then 'about 1 year'
    else "over #{(distance_in_minutes / 525600).round} years"
  end
end