CREATE TABLE users (
  user_id text PRIMARY KEY,
  name text,
  password text
);

CREATE TABLE followers (
  user_id text PRIMARY KEY,
  users set<text>
);

CREATE TABLE friends (
  user_id text PRIMARY KEY,
  users set<text>
);

CREATE TABLE oinks (
  oink_id timeuuid,
  user_id text,
  body text,
  PRIMARY KEY (oink_id, user_id)
);

CREATE TABLE timeline (
  user_id text,
  when timestamp,
  oink_id uuid,
  PRIMARY KEY (user_id, when)
);

CREATE TABLE userline (
  user_id text,
  when timestamp,
  oink_id uuid,
  PRIMARY KEY (user_id, when)
);

