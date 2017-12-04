CREATE TABLE IF NOT EXISTS records(
  -- 767 is the default max key length in MySQL
  -- https://dev.mysql.com/doc/refman/5.7/en/create-index.html
  id VARCHAR(767) PRIMARY KEY,
  parent VARCHAR(767) DEFAULT NULL,
  value LONGBLOB,
  INDEX (parent),
  FOREIGN KEY (parent) REFERENCES records(id) ON DELETE CASCADE
)
