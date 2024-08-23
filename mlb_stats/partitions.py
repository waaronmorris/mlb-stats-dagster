import datetime as dt

import dagster as dg

TIMECODE_FORMAT = '%Y%m%d_%H%M%S'

HOURLY_PARTITIONED_CONFIG = dg.HourlyPartitionsDefinition(start_date=f"{dt.datetime(2020, 1, 1):%Y-%m-%d-%H:%M}")

DAILY_PARTITIONED_CONFIG = dg.DailyPartitionsDefinition(start_date=f"{dt.datetime(2020, 1, 1):%Y-%m-%d}")

OTTONEU_PARTITIONED_CONFIG = dg.DailyPartitionsDefinition(start_date=f"{dt.datetime(2024, 8, 5):%Y-%m-%d}")
