-- table: plan_change
-- description: Each PLAN_CHANGE event is logged in this table
-- table_type: "bronze"
-- columns:
--      date: TIMESTAMP
--      account: TEXT
--      level: TEXT
--      details_change_date: TIMESTAMP
--      details_from: TEXT
--      details_to: TEXT


SELECT
    (raw->>'date')::TIMESTAMP AS date,
    (raw->>'account') AS account,
    (raw->>'level') AS level,
    (raw->'details'->>'change_date')::TIMESTAMP AS details_change_date,
    (raw->'details'->>'from') AS details_from,
    (raw->'details'->>'to') AS details_to
FROM logs
WHERE raw->>'log_type' = 'PLAN_CHANGE'

