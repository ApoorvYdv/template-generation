from dagster import Out, Output, op, graph
import pandas as pd
import logging

from fpdf import FPDF
import os

INPUT_DIRECTORY = 'pdf_generated'


@op(out={"df1": Out(is_required=False), "df2": Out(is_required=False), "df3": Out(is_required=False)})
def read_details(context):
    tem1 = []
    tem2 = []
    tem3 = []
    df = pd.read_csv("etl/data/data.csv")
    print(df.head())
    for ind in df.index:
        if(df['T1'][ind]=='Y'):
            tem1.append({'ID': df['ID'][ind], 'Name': df['name'][ind], 'Violation': df['Violation'][ind]})
        if(df['T2'][ind]=='Y'):
            tem2.append({'ID': df['ID'][ind], 'Name': df['name'][ind], 'Violation': df['Violation'][ind]})
        if(df['T3'][ind]=='Y'):
            tem3.append({'ID': df['ID'][ind], 'Name': df['name'][ind], 'Violation': df['Violation'][ind]})
    df1 = pd.DataFrame(tem1)
    df2 = pd.DataFrame(tem2)
    df3 = pd.DataFrame(tem3)
    context.log.info("Dataframe created!!!")
    
    yield Output(df1, "df1")
    yield Output(df2, "df2")
    yield Output(df3, "df3") 

@op
def create_temp1(context, df):
  lgr = logging.getLogger("console_logger")
  for index, row in df.iterrows():
      #create FPDF object
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
      context.log.info("pdf created!!!")

@op
def create_temp2(context, df):
  lgr = logging.getLogger("console_logger")
  for index, row in df.iterrows():
      #create FPDF object
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
      context.log.info("pdf created!!!")

@op
def create_temp3(context, df):
  lgr = logging.getLogger("console_logger")
  for index, row in df.iterrows():
      #create FPDF object
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
      context.log.info("pdf created!!!") 

@graph
def creating_pdf(t1, t2, t3):
  create_temp1(t1)
  create_temp2(t2)
  create_temp3(t3)