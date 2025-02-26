"""MLB Stats Dagster Project"""

from mlb_stats.config import load_environment, config

# Load environment variables when the package is imported
load_environment()

__all__ = ["config"]

import warnings

import dagster as dg
import duckdb

warnings.filterwarnings("ignore", category=dg.ExperimentalWarning)

from dagster_dbt import DbtCliResource

# from . import assets, io, jobs, resources, project, schedules

import mlb_stats.project as project
import mlb_stats.assets as assets
import mlb_stats.resources as resources
import mlb_stats.jobs as jobs
import mlb_stats.schedules as schedules
import mlb_stats.sensors as sensors
import  mlb_stats.io as io

from langchain_anthropic import ChatAnthropic


def create_ddb():
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""CREATE SECRET (
    TYPE R2,
    KEY_ID '{dg.EnvVar("CLOUDFLARE_CLIENT_ACCESS_KEY").get_value()}',
    SECRET '{dg.EnvVar("CLOUDFLARE_CLIENT_SECRET").get_value()}',
    ACCOUNT_ID '{dg.EnvVar("CLOUDFLARE_ACCOUNT_ID").get_value()}' -- your 33 character hexadecimal account ID
    );""")

    return conn


defs = dg.Definitions(
    assets=assets.all_assets,
    jobs=jobs.jobs(),
    schedules=schedules.generate_schedules(),
    sensors=sensors.sensors,
    resources={
        'fantasy_loader': resources.FantasyLoader(
            base_url="https://fantasy-baseball-loader-5yibsupwla-uc.a.run.app/"
        ),
        'cloudflare': resources.Cloudflare(
            bucket=dg.EnvVar("CLOUDFLARE_BUCKET").get_value(),
            account_id=dg.EnvVar("CLOUDFLARE_ACCOUNT_ID").get_value(),
            client_access_key=dg.EnvVar("CLOUDFLARE_CLIENT_ACCESS_KEY").get_value(),
            client_secret=dg.EnvVar("CLOUDFLARE_CLIENT_SECRET").get_value()
        ),
        'duckdb_io_manager': io.DuckPondIOManager(
            bucket_name=dg.EnvVar("CLOUDFLARE_BUCKET").get_value(),
            account_id=dg.EnvVar("CLOUDFLARE_ACCOUNT_ID").get_value(),
            client_access_key=dg.EnvVar("CLOUDFLARE_CLIENT_ACCESS_KEY").get_value(),
            client_secret=dg.EnvVar("CLOUDFLARE_CLIENT_SECRET").get_value(),
            duckdb=create_ddb()
        ),
        "dbt": DbtCliResource(project_dir=project.mlb_stats_dbt_project)
    }
)
