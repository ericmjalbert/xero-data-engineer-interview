-- table: user_location
-- description: Cleaned up geo information from user SIGNUP address
-- table_type: "silver"
-- columns:
--      account: TEXT
--      date: TIMESTAMP
--      address: TEXT
--      province: TEXT


-- To get a specific part of the string we need to count the number of whitespaces
WITH count_spaces AS (
    SELECT
        account,
        date,
        address,
        LENGTH(address) - LENGTH(REPLACE(address, ' ', '')) AS count_spaces
    FROM signup
),

-- This splits the address into the n-1 part, which normally contains the province
split_province_part AS (
    SELECT
        account,
        date,
        address,
        SPLIT_PART(address, ' ', count_spaces-1) AS province
    FROM count_spaces
)

-- In some edgecases, there is not white space for n-1 part, so this handles those cases
SELECT
    account,
    date,
    address,
    CASE WHEN LENGTH(province) > 2 THEN SPLIT_PART(province, ',', 2)
        ELSE province
        END AS province
FROM split_province_part
