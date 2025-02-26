{{ config(
        materialized='external',
        location='r2://mlb-stats/stage/ottoneu/player_roster_over_time.parquet'
        )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['mlbam_id', 'effective_date']) }} as player_roster_id,
   mlbam_id                                                               as player_id,
   name                                                                   as name,
   p.positions                                                            as positions,
   IFNULL(team_id, -999)                                                  as team_id,
   IFNULL(team_name, 'FA')                                                as team_name,
   salary                                                                 as salary,
   IFNULL(effective_date,  '1900-01-01T00:00:00.000Z')          as effective_date,
   IFNULL( expiration_date,  '9999-12-31T00:00:00.000Z')                  as expiration_date,
    FROM
        {{ ref("ottoneu_player_list") }}                  p
            LEFT JOIN {{ ref("ottoneu_roster_journey") }} t
                          ON p.ottoneu_id = t.ottoneu_id
