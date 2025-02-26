import dagster as dg
import pandas as pd
from plyball.mlb import MLBStats

import mlb_stats.configurations as configs
import mlb_stats.partitions as partitions

mlb_stats = MLBStats()

TIMECODE_FORMAT = '%Y%m%d_%H%M%S'


@dg.asset(
    compute_kind='python',
    key_prefix=['raw', 'mlb'],
    name="schedule",
    description="Schedule of MLB games",
    group_name="raw_mlb_api",
    io_manager_key="duckdb_io_manager",
    partitions_def=partitions.DAILY_PARTITIONED_CONFIG,
    tags={
        "source": "mlb_api",
        "data-tier": "raw",
        'dagster/max_runtime': '3600',
    },
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

    df.columns = (df.columns
                  .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)  # Convert camelCase to snake_case
                  .str.replace('[^a-zA-Z0-9_]', '',
                               regex=True)  # Ensure column names are qualified SQL column names
                  .str.replace('[^a-zA-Z0-9]', '_',
                               regex=True)  # Replace non-alphanumeric characters with underscores
                  .str.replace('^[0-9]', '_', regex=True)  # Prepend underscore if column name starts with a digit
                  .str.replace('_+', '_', regex=True)  # Remove multiple underscores
                  .str.lower()  # Convert to lowercase
                  )

    df['game_date'] = pd.to_datetime(df['game_date'])

    df = df[df['game_date'].dt.date == start_date.date()]

    df['load_time'] = dt.datetime.utcnow().isoformat()
    df['partition_key'] = context.partition_key
    df = df.sort_values('load_time')

    summary = df.describe()

    # Log the Materialization of the asset

    return dg.Output(
        value=df,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown(disable_numparse=True,)),
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
    key_prefix=['stage', 'mlb', 'schedule'],
    name="monthly",
    description="Schedule of MLB games",
    group_name="stg_mlb_schedule",
    io_manager_key="duckdb_io_manager",
    partitions_def=partitions.MONTHLY_PARTITIONED_CONFIG,
    ins={
        'raw_mlb_stats_schedule': dg.AssetIn(
            key=['raw', 'mlb', 'schedule'],
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=0,
                end_offset=0,
                allow_nonexistent_upstream_partitions=True
            )
        ),
    },
    tags={
        "source": "mlb_api",
        "data-tier": "stage",
        'dagster/max_runtime': '3600',
    },
)
def stage_monthly_mlb_stats_schedule(
        context,
        config: configs.ScheduleLoaderConfig,
        raw_mlb_stats_schedule: pd.DataFrame
):
    __start_process_time = dt.datetime.now()

    if len(raw_mlb_stats_schedule) == 0:
        return dg.Output(value=None,
                         tags={
                             "source": "raw",
                             "data-tier": "raw",
                         },
                         metadata={
                             "summary": dg.MetadataValue.md("No games scheduled"),
                             "rows": 0,
                             "columns": 0,
                             "columns_names": [],
                             "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
                         })

    raw_mlb_stats_schedule = raw_mlb_stats_schedule.convert_dtypes()

    summary = raw_mlb_stats_schedule.describe()

    return dg.Output(
        value=raw_mlb_stats_schedule.sort_values('load_time'),
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown(disable_numparse=True,)),
            "rows": len(raw_mlb_stats_schedule),
            "columns": len(raw_mlb_stats_schedule.columns),
            "columns_names": list(raw_mlb_stats_schedule.columns),
            "game_count": raw_mlb_stats_schedule['game_pk'].nunique(),
            "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
        },
        tags={
            "source": "raw",
            "data-tier": "stage",
        }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=['stage', 'mlb', 'schedule'],
    name="schedule",
    description="Schedule of MLB games",
    group_name="stg_mlb_schedule",
    io_manager_key="duckdb_io_manager",
    ins={
        'stg_mlb_schedule_monthly': dg.AssetIn(
            key=['stage', 'mlb', 'schedule', 'monthly'],
            partition_mapping=dg.AllPartitionMapping()
        ),
    },
    tags={
        "source": "mlb_api",
        "data-tier": "stage",
        'dagster/max_runtime': '3600',
    },
)
def stage_mlb_stats_schedule(
        context,
        config: configs.ScheduleLoaderConfig,
        stg_mlb_schedule_monthly: pd.DataFrame
):
    __start_process_time = dt.datetime.now()

    if len(stg_mlb_schedule_monthly) == 0:
        return dg.Output(value=None,
                         tags={
                             "source": "raw",
                             "data-tier": "raw",
                         },
                         metadata={
                             "summary": dg.MetadataValue.md("No games scheduled"),
                             "rows": 0,
                             "columns": 0,
                             "columns_names": [],
                             "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
                         })

    stg_mlb_schedule_monthly = stg_mlb_schedule_monthly.convert_dtypes()

    summary = stg_mlb_schedule_monthly.describe()

    return dg.Output(
        value=stg_mlb_schedule_monthly.sort_values('load_time'),
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown(disable_numparse=True,)),
            "rows": len(stg_mlb_schedule_monthly),
            "columns": len(stg_mlb_schedule_monthly.columns),
            "game_count": stg_mlb_schedule_monthly['game_pk'].nunique(),
            "columns_names": list(stg_mlb_schedule_monthly.columns),
            "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
        },
        tags={
            "source": "raw",
            "data-tier": "stage",
        }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=['raw', 'mlb'],
    name="games",
    description="List of MLB teams",
    group_name="raw_mlb_api",
    io_manager_key="duckdb_io_manager",
    partitions_def=partitions.DAILY_PARTITIONED_CONFIG,
    ins={
        'raw_mlb_stats_schedule': dg.AssetIn(
            key=['raw', 'mlb', 'schedule'],
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=-1,
                end_offset=-1,
                allow_nonexistent_upstream_partitions=True
            )
        ),
    },
    tags={
        "source": "mlb_api",
        "data-tier": "raw",
        'dagster/max_runtime': '3600',
    }
)
def raw_mlb_stats_games(
        context,
        raw_mlb_stats_schedule: pd.DataFrame):
    __start_process_time = dt.datetime.now()
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
                             "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
                         })

    rv_df = pd.DataFrame()
    context.log.info(f"Getting boxscore for {len(raw_mlb_stats_schedule)} games")
    for gamePk in raw_mlb_stats_schedule['game_pk'].dropna().to_list():
        context.log.info(f"Getting boxscore for gamePk: {gamePk}")
        df = mlb_stats.get_game_boxscore(str(int(gamePk)), dt.datetime.now())
        if df is None:
            return dg.Output(value=None)

        df['game_pk'] = gamePk
        df['game_date'] = raw_mlb_stats_schedule.loc[raw_mlb_stats_schedule['game_pk'] == gamePk, 'game_date'].values[0]
        df['partition_key'] = context.partition_key
        df['load_time'] = dt.datetime.utcnow()

        df.columns = (df.columns
                      .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)  # Convert camelCase to snake_case
                      .str.replace('[^a-zA-Z0-9_]', '',
                                   regex=True)  # Ensure column names are qualified SQL column names
                      .str.replace('[^a-zA-Z0-9]', '_',
                                   regex=True)  # Replace non-alphanumeric characters with underscores
                      .str.replace('^[0-9]', '_', regex=True)  # Prepend underscore if column name starts with a digit
                      .str.replace('_+', '_', regex=True)  # Remove multiple underscores
                      .str.lower()  # Convert to lowercase
                      )

        # Concat Row by Row
        try:
            rv_df = pd.concat([rv_df.reset_index(drop=True), df.reset_index(drop=True)], axis=0, ignore_index=True)
        except Exception as e:
            context.log.error(f"Error concatenating {gamePk}: {e}")
            context.log.error(f"Dataframe: {df.columns}")
            from collections import Counter
            columns = Counter(df.columns)
            # print([k for k, v in columns.items() if v > 1])
            context.log.error(f"Duplicate columns: {[k for k, v in columns.items() if v > 1]}")
            # print duplicate columns in the rv dataframe

    # print all the columns in the dataframe
    context.log.info(f"Columns: {rv_df.columns}")
    summary = rv_df.describe()

    return dg.Output(
        value=rv_df,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown(disable_numparse=True,)),
            "rows": len(rv_df),
            "columns": len(rv_df.columns),
            "player_count": rv_df['player_id'].nunique(),
            "game_count": rv_df['game_pk'].nunique(),
            "columns_names": list(rv_df.columns),
            "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
        },
        tags={
            "source": "fantasy_loader",
            "data-tier": "raw",
        }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=["stage", "mlb", "box_scores"],
    name="monthly",
    description="MLB Stats",
    group_name="stg_mlb_box_score",
    partitions_def=partitions.MONTHLY_PARTITIONED_CONFIG,
    io_manager_key="duckdb_io_manager",
    tags={
        "source": "mlb_api",
        "data-tier": "stage",
        'dagster/max_runtime': '3600',
    },
    ins={
        'raw_mlb_games': dg.AssetIn(
            key=['raw', 'mlb', 'games'],
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=0,
                end_offset=0,
                allow_nonexistent_upstream_partitions=True
            )
        ),
    },
)
def monthly_mlb_box_scores(
        context,
        raw_mlb_games: pd.DataFrame
):
    """
    Combine All MLB Stat Files into a single filez

    :param context:
    :param raw_mlb_games:
    :return:
    """
    __start_process_time = dt.datetime.now()

    if len(raw_mlb_games) == 0:
        return dg.Output(value=None,
                         tags={
                             "source": "raw",
                             "data-tier": "raw",
                         },
                         metadata={
                             "summary": dg.MetadataValue.md("No games scheduled"),
                             "rows": 0,
                             "columns": 0,
                             "columns_names": [],
                             "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
                         })


    raw_mlb_games = raw_mlb_games.convert_dtypes()

    # number_of players
    summary = raw_mlb_games.describe()

    return dg.Output(
        value=raw_mlb_games,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown(disable_numparse=True,)),
            "rows": len(raw_mlb_games),
            "columns": len(raw_mlb_games.columns),
            "player_count": raw_mlb_games['player_id'].nunique(),
            "game_count": raw_mlb_games['game_pk'].nunique(),
            "columns_names": list(raw_mlb_games.columns),
            "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
        },
        tags={
            "source": "raw",
            "data-tier": "stage",
        }
    )


@dg.asset(
    compute_kind='python',
    key_prefix=["stage", "mlb", 'box_scores'],
    name="box_score",
    description="MLB Stats",
    group_name="stg_mlb_box_score",
    io_manager_key="duckdb_io_manager",
    tags={
        "source": "mlb_api",
        "data-tier": "stage",
        'dagster/max_runtime': '3600',
    },
    ins={
        'monthly_box_scores': dg.AssetIn(
            key=["stage", "mlb", "box_scores", "monthly"],
            partition_mapping=dg.AllPartitionMapping()
        ),
    },
)
def box_scores(
        context,
        monthly_box_scores: pd.DataFrame
):
    """
    Combine All MLB Stat Files into a single file

    :param context:
    :param monthly_box_scores:
    :return:
    """
    __start_process_time = dt.datetime.now()

    if len(monthly_box_scores) == 0:
        return dg.Output(value=None,
                         tags={
                             "source": "raw",
                             "data-tier": "raw",
                         },
                         metadata={
                             "summary": dg.MetadataValue.md("No games scheduled"),
                             "rows": 0,
                             "columns": 0,
                             "columns_names": [],
                             "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
                         })

    monthly_box_scores = monthly_box_scores.convert_dtypes()

    # number_of players
    summary = monthly_box_scores.describe()


    return dg.Output(
        value=monthly_box_scores,
        metadata={
            "summary": dg.MetadataValue.md(summary.to_markdown(disable_numparse=True,)),
            "rows": len(monthly_box_scores),
            "columns": len(monthly_box_scores.columns),
            "player_count": monthly_box_scores['player_id'].nunique(),
            "game_count": monthly_box_scores['game_pk'].nunique(),
            "columns_names": list(monthly_box_scores.columns),
            "load_time": (dt.datetime.now() - __start_process_time).total_seconds()
        },
        tags={
            "source": "raw",
            "data-tier": "stage",
        }
    )


from mlb_stats.assets.mlb_stats.translations import *  # pylint: disable=wildcard-import
