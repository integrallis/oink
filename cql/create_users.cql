CREATE TABLE users (
  user_id text PRIMARY KEY,
  name text,
  password text,
  followers set<text>,
  friends set<text>
);