from dagster import Definitions, load_assets_from_modules, EnvVar
import duckdb

from . import assets
from . import io
from . import resources

all_assets = load_assets_from_modules([assets])

# Initialize the DuckDB instance
duckdb = duckdb.connect(database=":memory:", read_only=False)


defs = Definitions(
    assets=all_assets,
    resources={
        'fantasy_loader': resources.FantasyLoader(
            base_url="https://fantasy-baseball-loader-5yibsupwla-uc.a.run.app/"
        ),
        'cloudflare': resources.Cloudflare(
            bucket=EnvVar("CLOUDFLARE_BUCKET"),
            account_id=EnvVar("CLOUDFLARE_ACCOUNT_ID"),
            client_access_key=EnvVar("CLOUDFLARE_CLIENT_ACCESS_KEY"),
            client_secret=EnvVar("CLOUDFLARE_CLIENT_SECRET")
        ),
        'duckdb_io_manager': io.DuckPondIOManager(
            bucket_name=EnvVar("CLOUDFLARE_BUCKET"),
            account_id=EnvVar("CLOUDFLARE_ACCOUNT_ID"),
            client_access_key=EnvVar("CLOUDFLARE_CLIENT_ACCESS_KEY"),
            client_secret=EnvVar("CLOUDFLARE_CLIENT_SECRET"),
            duckdb=duckdb
        )
    }
)
