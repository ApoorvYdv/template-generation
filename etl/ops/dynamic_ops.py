from tkinter import NUMERIC
from dagster import Out, Output, op, graph, DynamicOutput, DynamicOut
import pandas as pd
import logging

from etl.db_con import get_postgres_creds

from fpdf import FPDF
import os

OUTPUT_DIRECTORY = 'pdf_generated'
INPUT_DIRECTORY = 'etl/data'

@op
def db_connection():
  engine = get_postgres_creds()
  conn = engine.connect()
  return conn

@op(out=DynamicOut(dict))
def dynamic_read_csv(context):
  path = f"{INPUT_DIRECTORY}/data.csv"
  df = pd.read_csv(path)
  for index, row in df.iterrows():
      row_detail = row.to_dict()
      yield DynamicOutput(
          value=row_detail,
          mapping_key=f"{index+1}",
      )

@op
def temp1(context, row):
  try:
    engine = get_postgres_creds()
    conn = engine.connect()
    lgr = logging.getLogger("console_logger")
    pdf = FPDF('P', 'mm', 'A4')
    lgr.error("pdf not created!!!")
    context.log.info('pdf creation started')
    #Add a page
    pdf.add_page()
    #specify font
    pdf.set_font('helvetica', '', 16)
    #create obj for each row in table
    idx = str(row['ID'])
    name = str(row['Name'])
    violation = str(row['Violation'])
    pdf.cell(150, 10, 'Template 3', ln=1)
    pdf.cell(150, 10, f'Hello {name}!')
    pdf.cell(80, 10, f'Greetings {idx}', ln=1)
    pdf.cell(0, 10, f'Your Violation: {violation}', ln=1)
    context.log.info('pdf is created now saving!!')
    pdf.output(f'pdf_generated/template_1/temp1_{idx}.pdf')
    conn.execute(f"INSERT INTO row_status (row_id, template_no, csv_file, status) VALUES ('{idx}.1', '1', '1', 'SUCCESS')")
    context.log.info("pdf created!!!")
    return True
  except Exception as e:
    conn.execute(f"INSERT INTO row_status (row_id, template_no, csv_file, status) VALUES ('{idx}.1', '1', '1', 'FAILURE')")
    context.log.info("row not processed")

@op
def temp2(context, row):
  try:
    engine = get_postgres_creds()
    conn = engine.connect()
    lgr = logging.getLogger("console_logger")
    pdf = FPDF('P', 'mm', 'A4')
    lgr.error("pdf not created!!!")
    context.log.info('pdf creation started')
    #Add a page
    pdf.add_page()
    #specify font
    pdf.set_font('helvetica', '', 16)
    #create obj for each row in table
    idx = str(row['ID'])
    name = str(row['Nam'])
    violation = str(row['Violation'])
    pdf.cell(150, 10, 'Template 3', ln=1)
    pdf.cell(150, 10, f'Hello {name}!')
    pdf.cell(80, 10, f'Greetings {idx}', ln=1)
    pdf.cell(0, 10, f'Your Violation: {violation}', ln=1)
    context.log.info('pdf is created now saving!!')
    pdf.output(f'pdf_generated/template_2/temp2_{idx}.pdf')
    conn.execute(f"INSERT INTO row_status (row_id, template_no, csv_file, status) VALUES ('{idx}.2', '2', '1', 'SUCCESS')")
    context.log.info("pdf created!!!")
    return True
  except Exception as e:
    conn.execute(f"INSERT INTO row_status (row_id, template_no, csv_file, status) VALUES ('{idx}.2', '2', '1', 'FAILURE')")
    print("indexGot", idx)

@op
def temp3(context, row):
  try:
    engine = get_postgres_creds()
    conn = engine.connect()
    lgr = logging.getLogger("console_logger")
    pdf = FPDF('P', 'mm', 'A4')
    lgr.error("pdf not created!!!")
    context.log.info('pdf creation started')
    #Add a page
    pdf.add_page()
    #specify font
    pdf.set_font('helvetica', '', 16)
    #create obj for each row in table
    idx = str(row['ID'])
    name = str(row['Name'])
    violation = str(row['Violation'])
    pdf.cell(150, 10, 'Template 3', ln=1)
    pdf.cell(150, 10, f'Hello {name}!')
    pdf.cell(80, 10, f'Greetings {idx}', ln=1)
    pdf.cell(0, 10, f'Your Violation: {violation}', ln=1)
    context.log.info('pdf is created now saving!!')
    pdf.output(f'pdf_generated/template_3/temp3_{idx}.pdf')
    conn.execute(f"INSERT INTO row_status (row_id, template_no, csv_file, status) VALUES ('{idx}.3', '3', '1', 'SUCCESS')")
    context.log.info("pdf created!!!") 
    return True
  except Exception as e:
    conn.execute(f"INSERT INTO row_status (row_id, template_no, csv_file, status) VALUES ('{idx}.3', '3', '1', 'FAILURE')")
    context.log.info("row not processed")

@op
def upload_t1_to_s3(v):
  return

@op
def upload_t2_to_s3(v):
  return

@op
def upload_t3_to_s3(v):
  return

@op(out={"T1": Out(is_required=False), "T2": Out(is_required=False), "T3": Out(is_required=False)})
def branching_templates(context, row):
  context.log.info(row)
  if row['T1'] == 'Y':
      yield Output(row, "T1")
  if row['T2'] == 'Y':
      yield Output(row, "T2")
  if row['T3'] == 'Y':
      yield Output(row, "T3")

@graph
def creating_dynamic_pdf(row):
  t1, t2, t3 = branching_templates(row)
  x1 = temp1(t1)
  x2 = temp2(t2)
  x3 = temp3(t3)
  upload_t1_to_s3(x1)
  upload_t2_to_s3(x2)
  upload_t3_to_s3(x3)