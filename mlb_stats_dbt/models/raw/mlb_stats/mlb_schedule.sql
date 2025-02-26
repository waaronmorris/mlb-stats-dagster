Select
    *
    FROM
        {{ source("cloudflare", "stg_mlb_schedule")}}
QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY load_time DESC) = 1
