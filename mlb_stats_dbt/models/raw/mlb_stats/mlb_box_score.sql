Select
    * EXCLUDE (load_time, game_date),
    try_strptime(game_date, ['%Y-%m-%dT%H:%M:%SZ']) as game_date
FROM {{ source("cloudflare", "stg_box_score") }} box_score
{#QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk,  ORDER BY load_time DESC) = 1#}