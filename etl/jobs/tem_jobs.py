from dagster import job, op

from etl.ops.tem_ops import creating_pdf, read_details

@job(
  tags={"dagster/max_retries": 3},
  # config={
  #       "execution": {
  #           "config": {
  #               "multiprocess": {
  #                   "max_concurrent": 2,
  #               },
  #           }
  #       }
  #   }
)
def temp_job():
  t1, t2, t3 = read_details()
  creating_pdf(t1, t2, t3)
