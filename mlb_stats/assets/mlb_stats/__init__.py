import datetime as dt

import dagster as dg
import pandas as pd
from plyball.mlb import MLBStats

import mlb_stats.configurations as configs
import mlb_stats.partitions as partitions
from mlb_stats import resources

mlb_stats = MLBStats()

TIMECODE_FORMAT = '%Y%m%d_%H%M%S'


@dg.asset(
    compute_kind='python',
    key_prefix=['raw'],
    name="mlb_stats_schedule",
    description="Schedule of MLB games",
    group_name="raw_mlb_api",
    io_manager_key="duckdb_io_manager",
    partitions_def=partitions.DAILY_PARTITIONED_CONFIG,
)
def raw_mlb_stats_schedule(
        context,
        config: configs.ScheduleLoaderConfig,
):
    rows_added = 0
    start_date = dt.datetime.strptime(context.partition_key, '%Y-%m-%d')
    end_date = start_date + dt.timedelta(days=1)
    df = mlb_stats.get_schedule(start_date=start_date, end_date=end_date)

    if df is None or len(df) == 0:
        return dg.Output(value=pd.DataFrame())

    df['load_time'] = dt.datetime.utcnow().isoformat()
    df = df.sort_values('load_time').drop_duplicates(['gamePk'], keep='first')

    summary = df.describe()

    # Log the Materialization of the asset

    return dg.Output(
        value=df,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown()),
            "rows": len(df),
            "columns": len(df.columns),
            "rows_added": len(df) - rows_added,
            "columns_names": list(df.columns),
        },
        tags={
            "source": "fantasy_loader",
            "data-tier": "raw",
        }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=['raw'],
    name="mlb_games",
    description="List of MLB teams",
    group_name="raw_mlb_api",
    io_manager_key="duckdb_io_manager",
    partitions_def=partitions.DAILY_PARTITIONED_CONFIG,
    ins={
        'raw_mlb_stats_schedule': dg.AssetIn(
            key=['raw', 'mlb_stats_schedule'],
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=-1,
                end_offset=0,
                allow_nonexistent_upstream_partitions=True
            )
        ),
    },
)
def raw_mlb_stats_games(
        context,
        raw_mlb_stats_schedule: pd.DataFrame,
        column_renamer: resources.ColumnRenamer):
    if len(raw_mlb_stats_schedule) == 0:
        return dg.Output(value=None,
                         tags={
                             "source": "fantasy_loader",
                             "data-tier": "raw",
                         },
                         metadata={
                             "summary": dg.MetadataValue.md("No games scheduled"),
                             "rows": 0,
                             "columns": 0,
                             "columns_names": [],
                         })

    rv_df = pd.DataFrame()
    context.log.info(f"Getting boxscore for {len(raw_mlb_stats_schedule)} games")
    for gamePk in raw_mlb_stats_schedule['gamePk'].dropna().to_list():
        context.log.info(f"Getting boxscore for gamePk: {gamePk}")
        df = mlb_stats.get_game_boxscore(str(int(gamePk)), dt.datetime.now())
        if df is None:
            return dg.Output(value=None)

        df['gamePk'] = gamePk
        df['load_time'] = dt.datetime.utcnow()
        # Concat Row by Row
        rv_df = pd.concat([rv_df, df], ignore_index=True)

    # number_of players

    columns = column_renamer.get_column_map(
        columns=rv_df.columns.to_list(),
        convertions=['snake_case']
    )

    rv_df = rv_df.rename(columns=columns.to_dict())

    summary = rv_df.describe()

    return dg.Output(
        value=rv_df,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown()),
            "rows": len(rv_df),
            "columns": len(rv_df.columns),
            "player_count": rv_df['player_id'].nunique(),
            "game_count": rv_df['gamePk'].nunique(),
            "columns_names": list(rv_df.columns),
            },
            tags={
                "source": "fantasy_loader",
                "data-tier": "raw",
            }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=["stage"],
    name="mlb_box_scores",
    description="MLB Stats",
    group_name="stg_mlb_box_score",
    io_manager_key="duckdb_io_manager",
    tags={
        "source": "fantasy_loader",
        "data-tier": "stage",
    },
    ins={
        'raw_mlb_games': dg.AssetIn(
            key=['raw', 'mlb_games'],
            partition_mapping=dg.AllPartitionMapping()
        ),
    },
)
def mlb_box_scores(context, raw_mlb_games: pd.DataFrame):
    if len(raw_mlb_games) == 0:
        return dg.Output(value=None)

    # number_of players
    summary = raw_mlb_games.describe()

    return dg.Output(
        value=raw_mlb_games,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown()),
            "rows": len(raw_mlb_games),
            "columns": len(raw_mlb_games.columns),
            "player_count": raw_mlb_games['player_id'].nunique(),
            "game_count": raw_mlb_games['gamePk'].nunique(),
            "columns_names": list(raw_mlb_games.columns),
        },
        tags={
            "source": "fantasy_loader",
            "data-tier": "raw",
        }
    )
