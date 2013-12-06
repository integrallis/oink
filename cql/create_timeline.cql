CREATE TABLE timeline (
  user_id text,
  when timestamp,
  oink_id uuid,
  PRIMARY KEY (user_id, when)
);