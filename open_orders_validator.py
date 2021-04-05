import pandas as pd 
import numpy as np 
import os 
import traceback
import sys
import pyodbc 
import smtplib
from datetime import datetime 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

#sql query to Open Order Header tables 
sql = ''' 
SELECT [COMPANY]
      ,[OR_ORDN]
      ,[OR_CHGN]
      ,[OR_BATCH]
      ,[OR_STS]
      ,[OR_CUST]
      ,[OR_CUSTPO]
      ,[OR_BULK]
      ,[OR_AMNT]
      ,[OR_OPEN]
      ,[OR_PERIOD]
      ,[OR_DATE]
      ,[OR_SHNB]
      ,[OR_SHBY]
      ,[OR_CANC]
      ,[OR_STORE]
      ,[OR_SHPDC]
      ,[OR_DEPT]
      ,[OR_SHPNAME]
      ,[OR_SHPADR1]
      ,[OR_SHPADR2]
      ,[OR_SHPADR3]
      ,[OR_SHPCITY]
      ,[OR_SHPSTATE]
      ,[OR_SHPZIP]
      ,[OR_SHPCTRY]
      ,[OR_SHPVIA]
      ,[OR_PRIORITY]
      ,[OR_WHSE]
      ,[OR_DIV]
      ,[OR_CODE]
      ,[OR_SOURCE]
      ,[OR_SEASON]
      ,[OR_REP1]
      ,[OR_COM1]
      ,[OR_REP2]
      ,[OR_COM2]
      ,[OR_REP3]
      ,[OR_COM3]
      ,[OR_TERMS]
      ,[OR_TAXC]
      ,[OR_AGING]
      ,[OR_CURRENCY]
      ,[OR_EXCHNG]
      ,[OR_EXCHNG2]
      ,[OR_CREDIT]
      ,[OR_CREDITNUM]
      ,[OR_HOLD]
      ,[OR_FACTOR]
      ,[OR_FACTAMNT]
      ,[OR_DISC]
      ,[OR_SHPCOMP]
      ,[OR_CNCBO]
      ,[OR_VEN]
      ,[OR_ASN_ST]
      ,[OR_O1]
      ,[OR_CREDITDTE]
      ,[OR_FGHT]
      ,[OR_SHIP_EARLY]
      ,[OR_PHONE]
      ,[OR_EMAIL]
      ,[OR_CARD]
      ,[OR_CARD_EXP]
      ,[OR_LST_PCK]
      ,[OR_REASON]
      ,[OR_CHGBY]
      ,[OR_CHGDATE]
      ,[OR_CHGPERIOD]
      ,[OR_ENTBY]
      ,[OR_ENTDATE]
      ,[OR_BILLTOST]
      ,[OR_CONTRACT]
      ,[OR_CUSTDIV]
      ,[OR_UCC]
      ,[OR_SALECD]
      ,[OR_BUYER]
      ,[OR_LAB]
      ,[OR_EDI_REFDTE]
      ,[OR_DISC2]
      ,[OR_CHGTIME]
      ,[OR_REF1]
      ,[OR_REF2]
      ,[OR_REF3]
      ,[OR_REF4]
      ,[or_taxp]
      ,[OR_UMSRP]
      ,[OR_OMINM]
      ,[or_cpn]
      ,[or_dchg_rea]
      ,[OR_PHF]
      ,[OR_OFC]
      ,[OR_EVENTCD]
      ,[OR_EVENTDTE]
      ,[OR_NEWSTORE]
      ,[OR_SIF]
      ,[Last_ETL_Date]
      ,[OR_ANY_CHG]
  FROM [Datamart].[ETL].[orders_table] #insert orders table
  WHERE (OR_SHPADR1 LIKE '%" %' OR OR_SHPADR2 LIKE '%" %' OR OR_SHPADR3 LIKE '%" %' OR OR_SHPCITY LIKE '%" %') and (OR_DATE BETWEEN '2020-02-01' and GETDATE() and OR_STS = 'O') 
  ORDER BY OR_DATE DESC '''


class Db:
    DRIVER   =  r'DRIVER={ODBC Driver 13 for SQL Server};'
    SERVER   =  r'SERVER=192.168.1.1,1433;'
    DATABASE =  r'DATABASE=Client;'
    Trusted_Connection = r'yes'

    def __init__(self):
        try:
            self.cnxn = pyodbc.connect(self.DRIVER + self.SERVER + 
                        self.DATABASE + self.Trusted_Connection)
        except Exception:
            self.__exit__(sys.exc_info())

        self.cursor = self.cnxn.cursor()
        self.execute = self.cursor.execute(sql)              

    def __enter__(self):
        return self

    #def __exit__(self, exc_type, exc_value, traceback):
    def __exit__(self, exc_msg):
        print("__exit__")
        print(exc_msg)
        self.cursor.close()
        self.cnxn.close() 

with Db() as d:
    print(d)
#check connections

pd.set_option('display.max.columns', 75)
df = pd.read_sql(sql, Db)

#generate open orders with double quotes on shipping information 
def openOrder_generators(): 
    path = r'C:\Users\david.han\OneDrive - <company name>\Desktop\Open Orders'
    new_update = os.path.join(path, 'OpenOrders_validation.csv')
    output_file = df.to_csv(new_update)
    return output_file

openOrder_generators() 

#send email to 
def send_email(): 
    fromaddr = "David.han@workemail.com"
    toaddr = "David.han@workemail.com"

    #more than one part listing
    msg = MIMEMultipart()

    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Open Order Address Fields Validation Report"

    body = "Please see attachments for weekly report for double quotes on Open Order Header - {}".format(datetime.date(datetime.now()))

    msg.attach(MIMEText(body, 'plain'))
#add path on where file is located
    filename = "OpenOrders_validation.csv"
    path = r'C:/Users/david.han/OneDrive - <company name>/Desktop/Open Orders/' + filename

    attachment = open(path, "rb")   

    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    msg.attach(part)

    #connect to outlook server
    try:
        server = smtplib.SMTP('smtp.outlook.com', 587)
        server.starttls()
        server.login('David.han@workemail.com', 'Enter Password')
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
    except Exception as e: 
        print('Couldnt connect to outlook server')
    finally: 
        server.quit() 

send_email()

