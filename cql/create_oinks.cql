CREATE TABLE oinks (
  oink_id timeuuid,
  user_id text,
  body text,
  PRIMARY KEY (oink_id, user_id)
);