-- table: login
-- description: Each LOGIN event is logged in this table
-- table_type: "bronze"
-- columns:
--      date: TIMESTAMP
--      account: TEXT
--      level: TEXT
--      details: TEXT


SELECT
    (raw->>'date')::TIMESTAMP AS date,
    (raw->>'account') AS account,
    (raw->>'level') AS level,
    (raw->>'details') AS details
FROM logs
WHERE raw->>'log_type' = 'LOGIN'


