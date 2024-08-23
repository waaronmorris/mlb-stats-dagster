
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='external', location='r2://mlb-stats/box_score.parquet') }}

Select *
FROM {{ source("mlb_stats","mlb_box_scores")}}