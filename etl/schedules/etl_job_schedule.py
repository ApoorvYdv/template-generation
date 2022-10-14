from dagster import schedule

from etl.jobs.tem_jobs import temp_job


@schedule(cron_schedule="*/30 * * * *", job=temp_job, execution_timezone="US/Central")
def etl_job_schedule(_context):
    run_config = {}
    return run_config
