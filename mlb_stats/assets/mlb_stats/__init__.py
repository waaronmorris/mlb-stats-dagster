import datetime as dt

import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBResource

import mlb_stats.configurations as configs
import mlb_stats.resources as resources
from mlb_stats.resources import RequestException

TIMECODE_FORMAT = '%Y%m%d_%H%M%S'


@dg.asset(
    name="mlb_schedule",
    description="Schedule of MLB games",
    group_name="mlb_stats",
    io_manager_key="s3_parquet_io_manager"
)
def mlb_stats_schedule(
        config: configs.ScheduleLoaderConfig,
        fantasy_loader: resources.FantasyLoader,
        cloudflare: resources.Cloudflare,
        duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    rows_added = 0
    start_date = config.start_date
    end_date = config.end_date
    endpoint = f"schedule"
    query = f"start_date={start_date}&end_date={end_date}"

    rv = fantasy_loader.request(endpoint=endpoint, query=query)

    try:
        rv_json = rv.json()
    except Exception as e:
        raise Exception(f"Failed to load schedule: {e} | {rv}")

    df = pd.DataFrame()

    for file in ['intermediate/mlb/schedule/schedule.latest.parquet', rv_json['file_name']]:
        try:
            append_df = cloudflare.get(file).sort_values('load_time').drop_duplicates(['gamePk'], keep='first')
            if file == rv_json['file_name']:
                rows_added = len(append_df)
            if df is not None:
                df = pd.concat([df, append_df], axis=0)
        except Exception as e:
            print(e)

    df = df.sort_values('load_time').drop_duplicates(['gamePk'], keep='first')

    cloudflare.put('intermediate/mlb/schedule/schedule.latest.parquet', df, )
    cloudflare.put(f'intermediate/mlb/schedule/schedule.{dt.datetime.utcnow().timestamp()}.parquet', df)

    # generate summary
    summary = df.describe()

    if rv_json['message'] == 'file_loaded':
        return dg.MaterializeResult(
            asset_key="mlb_schedule",
            metadata={
                "schedule_file": 'intermediate/mlb/schedule/schedule.latest.parquet',
                "last_schedule_file": f'intermediate/mlb/schedule/schedule.{dt.datetime.utcnow().timestamp()}.parquet',
                "last_raw_file": rv_json['file_name'],
                "summary": summary.to_markdown(),
                "rows": len(df),
                "columns": len(df.columns),
                "rows_added": len(df) - rows_added,
            },
            tags={
                "source": "fantasy_loader",
                "endpoint": endpoint
            },
            data_version=dg.DataVersion("1.0"),
        )

    raise Exception(f"Failed to load schedule: {rv}")