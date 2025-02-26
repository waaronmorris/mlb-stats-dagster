import dagster as dg

from mlb_stats.jobs import mlb_api


@dg.run_status_sensor(
    monitored_jobs=[mlb_api.raw_mlb_api_job],  # Specify the job to monitor
    run_status=dg.DagsterRunStatus.SUCCESS  # Specify the status to trigger on
)
def trigger_box_score_on_raw_success(context: dg.RunStatusSensorContext):
    # Check if the monitored job (raw_mlb_api_job) was successful
    if context.dagster_run.job_name == mlb_api.raw_mlb_api_job.name and context.dagster_run.status == dg.DagsterRunStatus.SUCCESS:
        # Trigger the box_score_job
        return dg.RunRequest(job_name=mlb_api.box_score_job.name, run_key=mlb_api.box_score_job.key, run_config={})

    return None


@dg.run_status_sensor(
    monitored_jobs=[mlb_api.raw_mlb_api_job],  # Specify the job to monitor
    run_status=dg.DagsterRunStatus.SUCCESS  # Specify the status to trigger on
)
def trigger_schedule_on_raw_success(context: dg.RunStatusSensorContext):
    # Check if the monitored job (raw_mlb_api_job) was successful
    if context.dagster_run.job_name == mlb_api.raw_mlb_api_job.name and context.dagster_run.status == dg.DagsterRunStatus.SUCCESS:
        # Trigger the box_score_job
        return dg.RunRequest(
            job_name=mlb_api.schedule_score_job.name,
            run_key=mlb_api.schedule_score_job.key,
                             run_config={})

    return None
