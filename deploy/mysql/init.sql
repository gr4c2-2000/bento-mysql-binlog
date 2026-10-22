CREATE DATABASE IF NOT EXISTS shop;

CREATE TABLE shop.orders (
  id      BIGINT PRIMARY KEY,
  status  VARCHAR(32)    NOT NULL,
  amount  DECIMAL(10, 2) NOT NULL
) ENGINE = InnoDB;

CREATE TABLE shop.customers (
  id    BIGINT PRIMARY KEY,
  email VARCHAR(255) NOT NULL
) ENGINE = InnoDB;

-- Tables outside the include_tables regex, used to prove the input filters
-- before anything reaches the pipeline.
CREATE DATABASE IF NOT EXISTS ops;
CREATE TABLE ops.heartbeat (
  id BIGINT PRIMARY KEY,
  at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = InnoDB;

CREATE USER 'repl'@'%' IDENTIFIED BY 'replpass';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
