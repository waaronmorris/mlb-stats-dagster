from dagster import Config

from pydantic import Field
import datetime as dt


class ScheduleLoaderConfig(Config):
    start_date: str = Field(
        description="The START_DATE date for the schedule",
        is_required=True,
        default= (dt.datetime.now() - dt.timedelta(days=30)).strftime('%Y%m%d')
    )
    end_date: str = Field(
        description="The end date for the schedule",
        is_required=True,
        default= dt.datetime.now().strftime('%Y%m%d')
    )


