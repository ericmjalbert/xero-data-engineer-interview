-- table: province_by_year
-- description: Yearly aggregation of the province signups and active_users (based on year end)
-- notes: Normally I'd aggregate this as daily/hourly so it can be used in multiple
--     places, but since we have the "active_users" value, it can change at the
--     end of the year and this way we don't have to replace past data at each
--     hour (ie. active users from '2019-04-11' might become churned on
--     '2019-05-11' and thus at the yearly aggregatation we'd get a different
--     result)
-- table_type: "gold"
-- columns:
--      year: TIMESTAMP
--      province: TEXT
--      signups: INTEGER
--      active_users: INTEGER


SELECT
    DATE_TRUNC('year', ul.date at time zone 'est') AS year,
    ul.province,
    COUNT(DISTINCT ul.account) AS signups,
    COUNT(DISTINCT ul.account) - COUNT(DISTINCT ce.account) AS active_users
FROM user_location ul
LEFT JOIN churn_event AS ce
    ON ce.account = ul.account
GROUP BY 1, 2
