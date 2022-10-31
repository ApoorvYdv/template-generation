import os
import csv
import random
import pandas as pd
from dagster import job, op, get_dagster_logger, repository, RetryRequested, RetryPolicy, graph, Out, Output
@op
def load_csv(_):
   filename = os.path.join(os.path.dirname(__file__), "franklin.csv")
   df = pd.read_csv(filename)
   return df
@op(out={"T1": Out(is_required=False), "T2": Out(is_required=False), "T3": Out(is_required=False)})
def branching_templates(datacsv):
    for index, row in datacsv.iterrows():
        num1 = row['T1']
        num2=row['T2']
        num3=row['T3']
        if num1 == 'Y':
            yield Output(1, "T1")
        if num2 == 'Y':
            yield Output(2, "T2")
        if num3 == 'Y':
            yield Output(3, "T3")
@op
def T1_op(_input):
    return 1
@op
def T2_op(_input):
    return 2
@op
def T3_op(_input):
    return 3
@graph(tags={"dagster/max_retries": 3})
def template(datacsv):
    T1,T2,T3=branching_templates(datacsv)
    T1_op(T1)
    T2_op(T2)
    T3_op(T3)
@job
def collective_job():
    records=load_csv()
    template(records)