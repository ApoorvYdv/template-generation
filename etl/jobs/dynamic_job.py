from dagster import job, op

from etl.ops.dynamic_ops import dynamic_read_csv, creating_dynamic_pdf, db_connection

@job(
  tags={"dagster/max_retries": 3},
  config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,
                },
            }
        }
    }
)
def dynamic_job():
#   db_connection()
  dynamic_read_csv().map(creating_dynamic_pdf)