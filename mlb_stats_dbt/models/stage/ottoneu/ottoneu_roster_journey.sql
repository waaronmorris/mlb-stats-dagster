{{ config(
        materialized='external',
        location='r2://mlb-stats/stage/ottoneu/player_roster_journey.parquet'
        )
}}

WITH
    journey AS (
    SELECT
        ottoneu_id
      , player_name
      , team_id
      , team_name
      , salary
      , transaction_type
      ,
    date
FROM
    {{ ref("ottoneu_transaction_math") }}
WHERE
    transaction_type not in (  'cut', 'cut (C)' , 'move')

UNION
DISTINCT

SELECT
    ottoneu_id
  , player_name
  , NULL as team_id
  , NULL as team_name
  , 0    AS salary
  , transaction_type
  , date
FROM
    {{ ref("ottoneu_transaction_math") }}
WHERE
    transaction_type in ( 'cut'  , 'cut (C)')

UNION
DISTINCT

SELECT
    ottoneu_id
  , player_name
  , team_id
  , team_name
  , salary
  , transaction_type
  , date
FROM
    {{ ref("ottoneu_transaction_math") }}
WHERE
    transaction_type in (  'move')
  and transaction_value = 1
    )

Select
    ottoneu_id
  , player_name
  , team_id
  , team_name
  , salary
  , transaction_type
  , date
  , date as effective_date
  , LEAD(
    date
  , 1) OVER (
    PARTITION BY ottoneu_id ORDER BY date) as expiration_date
FROM
    journey

UNION
DISTINCT

Select
    ottoneu_id
  , player_name
  , LAG(team_id, 1) OVER (PARTITION BY ottoneu_id ORDER BY date) as team_id
  , LAG(team_name, 1) OVER (PARTITION BY ottoneu_id ORDER BY date) as team_name
  , LAG(salary, 1) OVER (PARTITION BY ottoneu_id ORDER BY date) as salary
  , LAG(transaction_type, 1) OVER (PARTITION BY ottoneu_id ORDER BY date) as transaction_type
  , date
  , CASE
        WHEN LAG(date, 1) OVER (PARTITION BY ottoneu_id ORDER BY date) IS NULL
            THEN '1901-01-01T00:00:00.000Z'
        ELSE LAG(date, 1) OVER (PARTITION BY ottoneu_id ORDER BY date)
END
as effective_date,
    date as expiration_date
FROM journey
ORDER BY date ASC, effective_date ASC