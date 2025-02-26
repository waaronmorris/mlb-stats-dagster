import datetime as dt

import dagster as dg
import pandas as pd
import requests


@dg.asset(
    compute_kind='python',
    key_prefix=["raw", "mlb", 'translations'],
    name="game_types",
    description="MLB Stats",
    group_name="stg_mlb_translations",
    io_manager_key="duckdb_io_manager",
    tags={
        "source": "mlb_api",
        "data-tier": "raw",
        'dagster/max_runtime': '3600',
    }
)
def game_types(
        context
):
    """
    Combine All MLB Stat Files into a single file

    :param context:
    :param monthly_box_scores:
    :return:
    """
    __start_process_time = dt.datetime.now()

    url = "https://statsapi.mlb.com/api/v1/gameTypes"

    rv_game_types = requests.get(url).json()

    return dg.Output(
        value=pd.DataFrame(rv_game_types),
        metadata={
            "summary": dg.MetadataValue.md(rv_game_types.describe().to_markdown()),
            "rows": len(rv_game_types),
            "columns": len(rv_game_types.columns),
            "columns_names": list(rv_game_types.columns),
            "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
        },
        tags={
            "source": "raw",
            "data-tier": "mlb",
            'dagster/max_runtime': '3600',

        }
    )
