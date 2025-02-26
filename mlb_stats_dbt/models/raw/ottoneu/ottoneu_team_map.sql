{{ config(
        materialized='external',
        location='r2://mlb-stats/stage/ottoneu/team_map.parquet'
        )
}}

SELECT team_id, team_name
    FROM
        {{ source("cloudflare", "raw_league_transactions")}}
    GROUP BY
        1, 2