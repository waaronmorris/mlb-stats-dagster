import dagster as dg

raw_mlb_api_job = dg.define_asset_job(
    name="raw_mlb_api_job",
    selection=dg.AssetSelection.groups('raw_mlb_api')
)

box_score_job = dg.define_asset_job(
    name="box_scores",
    selection=dg.AssetSelection.groups('stg_mlb_box_score')
)
