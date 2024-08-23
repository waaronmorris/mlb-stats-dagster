import dagster as dg

from mlb_stats.jobs import  mlb_api


@dg.sensor(job=mlb_api.box_score_job)
def box_scores_sensor(context: dg.RunStatusSensorContext):
    if context.dagster_run.status == dg.DagsterRunStatus.SUCCESS:
        return dg.RunRequest(run_key=None, run_config={})
    return None
