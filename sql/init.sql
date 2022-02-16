CREATE TABLE users (
  user_id INT PRIMARY KEY,
  email VARCHAR,
  date_registration TIMESTAMP
);

CREATE TABLE transactions (
  user_id INT,
  price FLOAT
);

CREATE TABLE webinar (
  email VARCHAR
);

SELECT users.email, SUM(transactions.price) AS price_sum FROM users
JOIN transactions ON transactions.user_id = users.user_id
JOIN webinar ON webinar.email = users.email
AND users.date_registration > '2016-04-01 00:00:00'
AND users.email NOT IN (SELECT users.email FROM users WHERE users.date_registration < '2016-04-01 00:00:00')
GROUP BY users.email;

SELECT users.user_id, SUM(transactions.price) AS price_sum FROM users
JOIN transactions ON transactions.user_id = users.user_id
JOIN webinar ON webinar.email = users.email
AND users.date_registration > '2016-04-01 00:00:00'
AND users.email NOT IN (SELECT users.email FROM users WHERE users.date_registration < '2016-04-01 00:00:00')
GROUP BY users.user_id;

CREATE TABLE total_sum (
  user_id INT PRIMARY KEY,
  price_sum FLOAT
);

CREATE TABLE total_sum_by_email (
  email VARCHAR,
  price_sum FLOAT
);