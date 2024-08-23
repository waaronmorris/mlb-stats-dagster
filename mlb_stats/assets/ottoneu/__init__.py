import re
from datetime import datetime

import dagster as dg
import pandas as pd
from plyball.ottoneu import Ottoneu
from mlb_stats.partitions import OTTONEU_PARTITIONED_CONFIG


@dg.asset(
    compute_kind='python',
    key_prefix=['raw'],
    name="ottoneu_player_universe",
    description="Player Universe for Ottoneu",
    group_name="raw_ottoneu",
    io_manager_key="duckdb_io_manager",
    tags={
        "source": "dropbox_csv",
        "data-tier": "raw",
    },
)
def ottoneu_player_universe(context) -> dg.Output:
    url = 'https://www.dropbox.com/s/l3hegihwb0dt6xq/player_universe.csv?dl=1'
    rv_df = pd.read_csv(url, dtype=str)

    rv_df.columns = [re.sub("^[0-9]", "_" + column[0], column, count=0, flags=0) for column in rv_df.columns]
    rv_df.columns = [re.sub(r"\W+", "_", column) for column in rv_df.columns]
    rv_df.columns = [c.lower().replace(" ", "_") for c in rv_df.columns]
    rv_df['load_time'] = datetime.utcnow()

    summary = rv_df.describe()

    return dg.Output(
        value=rv_df,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown()),
            "rows": len(rv_df),
            "columns": len(rv_df.columns),
            "player_count": rv_df['ottoneu_id'].nunique(),
            "columns_names": list(rv_df.columns),
        },
        tags={
            "source": "dropbox_csv",
            "data-tier": "raw",
        }
    )


@dg.multi_asset(
    compute_kind='python',
    partitions_def=OTTONEU_PARTITIONED_CONFIG,
    group_name="raw_ottoneu",
    outs={
        'player_info': dg.AssetOut(
            key=['raw', 'ottoneu_player_info'],
            description="Player Info for Ottoneu",
            io_manager_key="duckdb_io_manager",
            tags={
                "source": "ottoneu-scaper",
                "data-tier": "raw",
            },
            is_required=True,
        ),
        'player_stats': dg.AssetOut(
            key=['raw', 'ottoneu_player_stats'],
            description="Player Stats for Ottoneu",
            io_manager_key="duckdb_io_manager",
            tags={
                "source": "ottoneu-scaper",
                "data-tier": "raw",
            },
            is_required=True,
        ),
        'batter_info': dg.AssetOut(
            key=['raw', 'ottoneu_batter_info'],
            description="Batter Info for Ottoneu",
            io_manager_key="duckdb_io_manager",
            tags={
                "source": "ottoneu-scaper",
                "data-tier": "raw",
            },
            is_required=True,
        ),
        'batter_stats': dg.AssetOut(
            key=['raw', 'ottoneu_batter_stats'],
            description="Batter Stats for Ottoneu",
            io_manager_key="duckdb_io_manager",
            tags={
                "source": "ottoneu-scaper",
                "data-tier": "raw",
            },
            is_required=True,
        ),
        'pitcher_info': dg.AssetOut(
            key=['raw', 'ottoneu_pitcher_info'],
            description="Pitcher Info for Ottoneu",
            io_manager_key="duckdb_io_manager",
            tags={
                "source": "ottoneu-scaper",
                "data-tier": "raw",
            },
            is_required=True,
        ),
        'pitcher_stats': dg.AssetOut(
            key=['raw', 'ottoneu_pitcher_stats'],
            description="Pitcher Stats for Ottoneu",
            io_manager_key="duckdb_io_manager",
            tags={
                "source": "ottoneu-scaper",
                "data-tier": "raw",
            },
            is_required=True,
        ),
    }
)
def ottoneu_players_search(context):
    ottoneu = Ottoneu(186)
    players = ottoneu.players()
    yield dg.Output(
        output_name='player_info',
        value=players['info'],
        metadata={
            "rows": len(players['info']),
            "columns": len(players['info'].columns),
            "player_count": players['info']['PlayerID'].nunique(),
            "columns_names": list(players['info'].columns),
            "sample": dg.MetadataValue.md(players['info'].head().to_markdown())
        },
        tags={
            "source": "ottoneu-scaper",
            "data-tier": "raw",
        }
    )

    yield dg.Output(
        output_name='player_stats',
        value=players['stat'],
        metadata={
            "rows": len(players['stat']),
            "columns": len(players['stat'].columns),
            "player_count": players['stat']['PlayerID'].nunique(),
            "columns_names": list(players['stat'].columns),
            "sample": dg.MetadataValue.md(players['stat'].head().to_markdown())
        },
        tags={
            "source": "ottoneu-scaper",
            "data-tier": "raw",
        }
    )

    yield dg.Output(
        output_name='batter_info',
        value=players['batter']['info'],
        metadata={
            "rows": len(players['batter']['info']),
            "columns": len(players['batter']['info'].columns),
            "player_count": players['batter']['info']['PlayerID'].nunique(),
            "columns_names": list(players['batter']['info'].columns),
            "sample": dg.MetadataValue.md(players['batter']['info'].head().to_markdown())
        },
        tags={
            "source": "ottoneu-scaper",
            "data-tier": "raw",
        }
    )

    yield dg.Output(
        output_name='batter_stats',
        value=players['batter']['stat'],
        metadata={
            "rows": len(players['batter']['stat']),
            "columns": len(players['batter']['stat'].columns),
            "player_count": players['batter']['stat']['PlayerID'].nunique(),
            "columns_names": list(players['batter']['stat'].columns),
            "sample": dg.MetadataValue.md(players['batter']['stat'].head().to_markdown())
        },
        tags={
            "source": "ottoneu-scaper",
            "data-tier": "raw",
        }
    )

    yield dg.Output(
        output_name='pitcher_info',
        value=players['pitcher']['info'],
        metadata={
            "rows": len(players['pitcher']['info']),
            "columns": len(players['pitcher']['info'].columns),
            "player_count": players['pitcher']['info']['PlayerID'].nunique(),
            "columns_names": list(players['pitcher']['info'].columns),
            "sample": dg.MetadataValue.md(players['pitcher']['info'].head().to_markdown())
        },
        tags={
            "source": "ottoneu-scaper",
            "data-tier": "raw",
        }
    )

    yield dg.Output(
        output_name='pitcher_stats',
        value=players['pitcher']['stat'],
        metadata={
            "rows": len(players['pitcher']['stat']),
            "columns": len(players['pitcher']['stat'].columns),
            "player_count": players['pitcher']['stat']['PlayerID'].nunique(),
            "columns_names": list(players['pitcher']['stat'].columns),
            "sample": dg.MetadataValue.md(players['pitcher']['stat'].head().to_markdown())
        },
        tags={
            "source": "ottoneu-scaper",
            "data-tier": "raw",
        }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=['stage'],
    name="ottoneu_player_information",
    description="Team Universe for Ottoneu",
    group_name="stg_ottoneu",
    io_manager_key="duckdb_io_manager",
    tags={
        "source": "dropbox_csv",
        "data-tier": "raw",
    },
    ins={
        'ottoneu_player_info': dg.AssetIn(
            key=['raw', 'ottoneu_player_info'],
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=-1,
                end_offset=0,
                allow_nonexistent_upstream_partitions=True
            )),
        'ottoneu_player_universe': dg.AssetIn(
            key=['raw', 'ottoneu_player_universe']
        )
    }
)
def ottoneu_player_information(context,
                               ottoneu_player_info: pd.DataFrame,
                               ottoneu_player_universe: pd.DataFrame,
                               ) -> dg.Output:
    if ottoneu_player_info is None:
        return dg.Output(value=None)

    if ottoneu_player_universe is None:
        return dg.Output(value=None)

    info_column_map = {
        'PlayerID': 'ottoneu_id',
        'TeamID': 'team_id',
        'TeamName': 'team_name',
        'PlayerName': 'player_name',
        'Positions': 'positions',
        'NoteIcon': 'note_icon',
        'OnWatchlist': 'on_watchlist',
        'OnDraftWatchlist': 'on_draft_watchlist',
        'Cost': 'cost',
        'CapPenalty': 'cap_penalty',
        'ProTeam': 'pro_team',
        'VotedOff': 'voted_off',
        'OwnershipPct': 'ownership_percentage',
        'OwnershipChanges': 'ownership_changes',
        'NewsIcon': 'news_icon',
        'OnDLHtml': 'on_dl_html',
        'OnRestrictedList': 'on_restricted_list',
        'AverageSalary': 'average_salary',
        'MedianSalary': 'median_salary',
        'Points': 'points',
        'PointsRate': 'points_rate',
    }

    info_df = ottoneu_player_info.rename(columns=info_column_map)

    universe_column_map = {
        'ottoneu_id': 'ottoneu_id',
        'name': 'name',
        'fg_id': 'fangraph_id',
        'fg_minor_id': 'fangraph_minor_id',
        'mlbam_id': 'mlbam_id',
        'birthday': 'birthday',
        'ottoneu_positions': 'positions',
        'load_time': 'load_time',
    }

    universe_df = ottoneu_player_universe.rename(columns=universe_column_map)

    rv_df = pd.merge(info_df, universe_df, on='ottoneu_id', how='left')

    return dg.Output(
        value=rv_df,
        metadata={
            "summary": dg.MetadataValue.md(rv_df.describe().to_markdown()),
            "rows": len(rv_df),
            "columns": len(rv_df.columns),
            "player_count": rv_df['PlayerID'].nunique(),
            "columns_names": list(rv_df.columns),
        },
        tags={
            "data-tier": "staging",
        }
    )
