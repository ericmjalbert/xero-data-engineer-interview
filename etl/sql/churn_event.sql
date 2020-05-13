-- table: churn_event
-- description: Measures the users that have churned and at what time did they last login
-- table_type: "silver"
-- columns:
--      account: TEXT
--      date: TIMESTAMP


-- Get next login and default it to future date to force churn in cases without a next login
WITH next_logins AS (
    SELECT
        account,
        date,
        date at time zone 'est' AS est_date,
        COALESCE(LEAD(date at time zone 'est') OVER (PARTITION BY account ORDER BY date), '9999-01-01') AS next_login
    FROM login
)

-- The idea is to only have users with a churn event in this table.
-- Only keep churns that happened over 30 days before the most recent event
-- since otherwise we do not have enough data to say if they churned or not
SELECT
    account,
    date AS churn_date
FROM next_logins
WHERE (next_login - est_date) > interval '30 days'
    AND (SELECT MAX(est_date) FROM next_logins) - est_date > interval '30 days'
