{{
    config(
        materialized='external',
        location='r2://mlb-stats/stage/dbt/mlb/box_score.parquet'
    )
}}

Select
    {{ dbt_utils.star(from=ref("mlb_box_score"), relation_alias="box_score") }},
    schedule.game_type,
    schedule.season,
    schedule.game_number,
    CONCAT(schedule.season, '.', schedule.game_number) as game_id,
    ROW_NUMBER() OVER (PARTITION BY player_id, schedule.season ORDER BY box_score.game_date DESC) AS player_game_number,
    CONCAT(schedule.season, '.', ROW_NUMBER() OVER (PARTITION BY player_id, schedule.season ORDER BY box_score.game_date DESC)) AS player_season_game_number,
    CAST(batting_at_bats AS DECIMAL) * -1.0 as ab_points,
    CAST(batting_hits AS DECIMAL) * 5.6 as h_points,
    CAST(batting_doubles AS DECIMAL) * 2.9 as doubles_points,
    CAST(batting_triples AS DECIMAL) * 5.7 as triples_points,
    CAST(batting_home_runs AS DECIMAL) * 9.4 as home_runs_points,
    CAST(batting_base_on_balls AS DECIMAL) * 3.0 as walks_points,
    CAST(batting_hit_by_pitch AS DECIMAL) * 3.0 as hits_by_pitch_points,
    CAST(batting_stolen_bases AS DECIMAL) * 1.9 as stolen_bases_points,
    CAST(batting_caught_stealing AS DECIMAL) * -2.8 as caught_stealing_points,
    CAST(pitching_innings_pitched AS DECIMAL) * 5.0 as innings_pitched_points,
    CAST(pitching_strike_outs AS DECIMAL) * 2.0 as strikeouts_points,
    CAST(pitching_base_on_balls AS DECIMAL) * -3.0 as walks_pitched_points,
    CAST(pitching_hit_batsmen AS DECIMAL) * -3.0 as hit_batsmen_points,
    CAST(pitching_home_runs AS DECIMAL) * -13.0 as home_runs_allowed_points,
    CAST(pitching_saves AS DECIMAL) * 5.0 as saves_points,
    CAST(pitching_holds AS DECIMAL) * 4.0 as holds_points
    FROM
        {{ ref("mlb_box_score") }} box_score
    LEFT JOIN {{ ref("mlb_schedule") }} schedule ON schedule.game_pk = box_score.game_pk
