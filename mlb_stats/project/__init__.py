from pathlib import Path

from dagster_dbt import DbtProject

import dotenv

dotenv.load_dotenv()

PROJECT_DIR = Path(__file__).joinpath("..", "..","..","mlb_stats_dbt").resolve()

PACKAGED_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt-project").resolve()
#
# print(PROJECT_DIR)
#
# print(PACKAGED_PROJECT_DIR)

mlb_stats_dbt_project = DbtProject(
    project_dir=PROJECT_DIR,
    # packaged_project_dir=PACKAGED_PROJECT_DIR,
)

mlb_stats_dbt_project.prepare_if_dev()
