import os
from dagster import job, op, get_dagster_logger, repository, RetryRequested, RetryPolicy, graph, Out, Output, DynamicOut, DynamicOutput
@op(out=DynamicOut(str))
def listTestCases(context):
    for test_case in range(10) :
        testcase = f"testcase_{test_case}"
        yield DynamicOutput(
            value=testcase,
            mapping_key=testcase,
        )
@op
def A(context,testcase) :
   context.log.info(f"[A] : {testcase}")
@op
def B(context,testcase) :
   context.log.info(f"[B] : {testcase}")
@op
def C(context,testcase) :
   context.log.info(f"[C] : {testcase}")
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
    })
def dynamic_job_test():
    listTestCases().map(A)