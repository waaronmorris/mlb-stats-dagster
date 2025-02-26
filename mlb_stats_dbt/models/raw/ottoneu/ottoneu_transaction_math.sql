{{ config(
        materialized='external',
        location='r2://mlb-stats/stage/ottoneu/transaction_math.parquet'
        )
}}

WITH
    transaction_math AS (
    SELECT
    date
  , trade_id
  , auction_id
  , player_id as ottoneu_id
  , player_name
  , team_id
  , team_name
  , salary
  , transaction_type
  , CASE
    WHEN transaction_type = 'add' THEN 1
    WHEN transaction_type = 'cut' THEN -1
    WHEN transaction_type = 'move' THEN 1
    WHEN transaction_type = 'increase' THEN 0
    WHEN transaction_type = 'add (C)' THEN 1
    WHEN transaction_type = 'cut (C)' THEN -1
    ELSE 0
END
as transaction_value
      , CASE
            WHEN transaction_type = 'add' THEN salary
            WHEN transaction_type = 'cut' THEN -1 * salary
            WHEN transaction_type = 'move' THEN salary
            WHEN transaction_type = 'increase' THEN salary - LAG(salary, 1) OVER (PARTITION BY player_id ORDER BY date)
            WHEN transaction_type = 'add (C)' THEN salary
            WHEN transaction_type = 'cut (C)' THEN -1 * salary
            ELSE 0
END
as salary_change
FROM  {{ ref("ottoneu_league_transactions") }}

UNION DISTINCT

SELECT
    date
  , trade_id
  , auction_id
  , player_id as ottoneu_id
  , player_name
  , team_map.team_id
  , from_team
  , salary
  , transaction_type
  , -1 as transaction_value
  , -1 * salary as salary_change
    FROM
        {{ ref("ottoneu_league_transactions") }} transactions
        LEFT JOIN {{ ref("ottoneu_team_map") }} team_map
ON team_map.team_name = transactions.from_team
    WHERE
        from_team is not null
        )

Select
    *
    FROM
        transaction_math
    ORDER BY date ASC