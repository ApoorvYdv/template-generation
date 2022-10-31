from dagster import Out, Output, op, graph, DynamicOutput, DynamicOut

from etl.db_con import get_postgres_creds

import pandas as pd
import logging
from fpdf import FPDF
import os

OUTPUT_DIRECTORY = 'pdf_generated'
INPUT_DIRECTORY = 'etl/data'


@op(out={"df1": Out(is_required=False), "df2": Out(is_required=False), "df3": Out(is_required=False)})
def read_details(context):
    engine = get_postgres_creds()
    conn = engine.connect()    
    df1 = pd.read_sql_query(sql = "SELECT * FROM row_status WHERE status='FAILURE'", con = conn)
    path = f"{INPUT_DIRECTORY}/data.csv"
    df2 = pd.read_csv(path)
    tem1 = []
    tem2 = []
    tem3 = []
    # for df in dataframes:
    for index, row in df1.iterrows():
        if(row['template_no']==1):
            tem1.append(df2.loc[int(row['row_id'])-1].to_dict())
        if(row['template_no']==2):
            tem2.append(df2.loc[int(row['row_id'])-1].to_dict())
        if(row['template_no']==3):
            tem3.append(df2.loc[int(row['row_id'])-1].to_dict())

    tdf1 = pd.DataFrame(tem1)
    tdf2 = pd.DataFrame(tem2)
    tdf3 = pd.DataFrame(tem3)
    context.log.info(tdf2)
    
    yield Output(tdf1, "df1")
    yield Output(tdf2, "df2")
    yield Output(tdf3, "df3") 

@op
def create_temp1(context, df):
  lgr = logging.getLogger("console_logger")
  engine = get_postgres_creds()
  conn = engine.connect() 
  for index, row in df.iterrows():
    #create FPDF object
    try:
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
      pdf.cell(150, 10, 'Template 1', ln=1)
      pdf.cell(150, 10, f'Hello {name}!', ln=1)
      pdf.cell(80, 10, f'Greetings {idx}', ln=1)
      pdf.cell(0, 10, f'Your Violation: {violation}', ln=1)
      context.log.info('pdf is created now saving!!')
      pdf.output(f'pdf_generated/template_1/temp1_{idx}.pdf')
      conn.execute(f"UPDATE row_status SET status = 'SUCCESS' WHERE row_id={idx}.1")
      context.log.info(row)
      context.log.info("pdf created!!!")
    except Exception as e:
      context.log.info("ERROR")

@op
def create_temp2(context, df):
  lgr = logging.getLogger("console_logger")
  engine = get_postgres_creds()
  conn = engine.connect() 
  for index, row in df.iterrows():
    try:
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
      pdf.cell(150, 10, 'Template 2', ln=1)
      pdf.cell(150, 10, f'Hello {name}!')
      pdf.cell(80, 10, f'Greetings {idx}', ln=1)
      pdf.cell(0, 10, f'Your Violation: {violation}', ln=1)
      context.log.info('pdf is created now saving!!')
      pdf.output(f'pdf_generated/template_2/temp2_{idx}.pdf')
      conn.execute(f"UPDATE row_status SET status = 'SUCCESS' WHERE row_id={idx}.2")
      context.log.info(f'{idx}.2')
      context.log.info("pdf created!!!")
    except Exception as e:
      context.log.info("Error")

@op
def create_temp3(context, df):
  lgr = logging.getLogger("console_logger")
  engine = get_postgres_creds()
  conn = engine.connect() 
  for index, row in df.iterrows():
    try:
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
      conn.execute(f"UPDATE row_status SET status = 'SUCCESS' WHERE row_id={idx}.2")
      context.log.info("pdf created!!!") 
    except Exception as e:
      context.log.info("ERROR")

@graph
def creating_pdf(df1, df2, df3):
    create_temp1(df1)
    create_temp2(df2)
    create_temp3(df3)