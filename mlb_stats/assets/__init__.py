import dagster as dg

from mlb_stats.assets import core
from mlb_stats.assets import dbt
from mlb_stats.assets import mlb_stats
from mlb_stats.assets import ottoneu

# from .mlb_stats import *  # pylint: disable=wildcard-import
# from .ottoneu import *  # pylint: disable=wildcard-import
# from .core import *  # pylint: disable=wildcard-import

all_assets = dg.load_assets_from_modules([
    core,
    dbt,
    mlb_stats,
    ottoneu
])

__all__ = [
    'mlb_stats',
    'ottoneu',
    'core',
    'dbt',
    'all_assets'
]
