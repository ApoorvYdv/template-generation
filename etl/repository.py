from dagster import repository

#etl job
from etl.jobs.tem_jobs import temp_job

#etl job schedule
from etl.schedules.etl_job_schedule import etl_job_schedule


@repository
def etl():
    tem_job = [temp_job]
    schedules = [etl_job_schedule]

    return tem_job + schedules
