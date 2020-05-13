-- table: signup
-- description: Each SIGNUP event is logged in this table
-- table_type: "bronze"
-- columns:
--      date: TIMESTAMP
--      account: TEXT
--      level: TEXT
--      plan: TEXT
--      firstname: TEXT
--      lastname: TEXT
--      address: TEXT


SELECT
    (raw->>'date')::TIMESTAMP AS date,
    (raw->>'account') AS account,
    (raw->>'level') AS level,
    (raw->>'plan') AS plan,
    (raw->>'firstname') AS firstname,
    (raw->>'lastname') AS lastname,
    (raw->>'address') AS address
FROM logs
WHERE raw->>'log_type' = 'SIGN_UP'


