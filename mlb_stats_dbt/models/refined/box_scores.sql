{{
    config(
        materialized='external',
        location='r2://mlb-stats/refined/box_scores.parquet'
    )
}}

SELECT
    bx.game_date
  , pr.player_roster_id
  , pr.player_id
  , pr.name
  , pr.positions
  , pr.team_id
  , pr.team_name
  , pr.salary
  , {{ dbt_utils.star(from=ref("mlb_player_box_scores"), relation_alias="bx") }}
  , (
        IFNULL(bx.ab_points, 0)
            + IFNULL(bx.h_points, 0)
            + IFNULL(bx.doubles_points, 0)
            + IFNULL(bx.triples_points, 0)
            + IFNULL(bx.home_runs_points, 0)
            + IFNULL(bx.walks_points, 0)
            + IFNULL(bx.hits_by_pitch_points, 0)
            + IFNULL(bx.stolen_bases_points, 0)
            + IFNULL(bx.caught_stealing_points, 0)
            + IFNULL(bx.innings_pitched_points, 0)
            + IFNULL(bx.strikeouts_points, 0)
            + IFNULL(bx.walks_pitched_points, 0)
            + IFNULL(bx.hit_batsmen_points, 0)
            + IFNULL(bx.home_runs_allowed_points, 0)
            + IFNULL(bx.saves_points, 0)
            + IFNULL(bx.holds_points, 0)
        )
                                                  as total_points
    FROM
        {{ ref("mlb_player_box_scores") }}                         bx
            LEFT JOIN {{ ref("ottoneu_player_roster_over_time") }} pr
                          ON bx.player_id = pr.player_id
                              AND game_date >= pr.effective_date
                              AND game_date < pr.expiration_date
