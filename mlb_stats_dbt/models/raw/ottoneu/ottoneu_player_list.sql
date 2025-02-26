{{ config(
        materialized='external',
        location='r2://mlb-stats/stage/ottoneu/player_list.parquet'
        )
}}

SELECT
    ottoneu_id,
    name,
    fg_id,
    fg_minor_id,
    mlbam_id,
    birthday,
    string_split(ottoneu_positions, '/') as positions
    FROM
        {{ source("cloudflare", "ottoneu_player_universe")}}