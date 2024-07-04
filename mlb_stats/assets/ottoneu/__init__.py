from dagster import (
    asset,
    MaterializeResult,
    DataVersion
)

from mlb_stats.resources import FantasyLoader


@asset(
    name="ottoneu_player_universe",
    description="Player Universe for Ottoneu",
    group_name="ottoneu"
)
def ottoneu_player_universe(fantasy_loader: FantasyLoader) -> MaterializeResult:
    endpoint = f"ottoneu/get_player_universe"

    rv = fantasy_loader.request(endpoint=endpoint, query="")

    try:
        rv_json = rv.json()
    except Exception as e:
        raise Exception(f"Failed to load player universe: {e} | {rv}")

    if rv_json['message'] == 'file_loaded':
        return MaterializeResult(
            asset_key="ottoneu_player_universe",
            metadata={
                "file_name": rv_json['file_name'],
            },
            tags={
                "source": "fantasy_loader",
                "endpoint": endpoint.replace("/", ".")
            },
            data_version=DataVersion(rv_json['file_name']),
        )

    raise Exception(f"Failed to load player universe: {rv}")
