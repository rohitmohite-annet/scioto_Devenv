import matplotlib.pyplot as plt
import warnings
import logging
warnings.filterwarnings("ignore")
from matplotlib.patches import FancyBboxPatch
import seaborn as sns
from datetime import datetime, timedelta,date
import pandas as pd
import base64
from io import BytesIO
from emailto_send import *
import pyodbc
from sqlalchemy import create_engine
import urllib
import json
from socketlabs.injectionapi import SocketLabsClient
from socketlabs.injectionapi.message.__imports__ import Attachment,BasicMessage,EmailAddress,BulkRecipient,BulkMessage

def sql_connection():
    server = 'epsql-srv-scioto-4see.database.windows.net'
    database = 'qasciotodb'
    username = 'sciotosqladmin'
    password = 'Ret$nQ2stkl21'
    cnxn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
current_date = datetime.now()

if int(current_date.strftime("%d")) > 20:
    current_month = datetime.now().month - 1

else:
    current_month = datetime.now().month - 2


def fetch_data_NOI():
    connection = sql_connection()
    print(str(year)[-2:])
    print(current_month)
    data = pd.read_sql("select  * from [dbo].[viewIncomeStatement] where PropertyManager <> ''  and  FiscalYear = {} and FiscalMonth = {} ".format(str(year)[-2:],current_month), connection)
    connection.close()
    return data

def persqft_data():
    connection = sql_connection()
    sqft = pd.read_sql("select [PropertyID],[PropertyManager],[PropertyPKID],[Company Description],[Property Status],[Property Type],[Unit Square Feet] from [dbo].[viewPropertyUnitLeaseDetails] where PropertyManager <> '' ",connection)
    connection.close()
    sqft.drop_duplicates(subset="PropertyID", inplace=True)

    Sqft_data = pd.DataFrame()
    for key, Promanage in enumerate(list(sqft['PropertyManager'].unique())):
        datasq = sqft[sqft['PropertyManager'] == Promanage]
        sqft_sum = datasq['Unit Square Feet'].sum()
        write_to_data = {'National_tenant': Promanage, 'Unit Square Feet': sqft_sum}
        dataframe_to_write = pd.DataFrame([write_to_data], columns=write_to_data.keys())
        Sqft_data = Sqft_data.append(dataframe_to_write, ignore_index=True)
    Sqft_data = Sqft_data.loc[Sqft_data['Unit Square Feet'] > 0]

    return Sqft_data

def insight_calculation(df):
    Revenue = df[df['L3'] == 'REVENUE']['Amount'].sum()
    Expenses = df[df['L3'] == 'OPERATING EXPENSES']['Amount'].sum()
    diff = (Revenue - Expenses)
    return round(diff, 2)


def calcluate():
    get_data = fetch_data_NOI()
    Final_dataframe = pd.DataFrame()
    for ind, i in enumerate(list(get_data['PropertyManager'].unique())):
        National_tenant_data = get_data[get_data['PropertyManager'] == i]
        NOI_sum = insight_calculation(National_tenant_data)
        KPI = 'NOI'
        towrite = {'Index': ind, 'National_tenant': i, 'KPI': KPI, 'YEAR': year, 'NOI_amount': NOI_sum}
        dataframe_to_write = pd.DataFrame([towrite], columns=towrite.keys())
        Final_dataframe = Final_dataframe.append(dataframe_to_write, ignore_index=True)
    # Final_dataframe = Final_dataframe[Final_dataframe.NOI_amount != 0]
    return Final_dataframe

def merge_with_sqft():
    check = calcluate()
    sqft = persqft_data()
    merged_sqft = check.merge(sqft, on='National_tenant', how='left')
    merged_sqft['NOI_Persqft'] = round((merged_sqft['NOI_amount']/merged_sqft['Unit Square Feet']),2)
    merged_sqft.dropna(subset=['NOI_Persqft'],inplace=True)
    merged_sqft = merged_sqft[['Index','National_tenant','KPI','YEAR','NOI_amount','Unit Square Feet','NOI_Persqft']]
    return merged_sqft

def current_top_5_property():
    current_year_data = merge_with_sqft()
    top5properties = current_year_data.sort_values(by=['NOI_Persqft'], ascending=False)[:5]['National_tenant'].to_list()
    top_5_values = current_year_data.sort_values(by=['NOI_Persqft'], ascending=False)[:5]['NOI_Persqft'].to_list()
    return top5properties,top_5_values


if __name__=='__main__':
    try:
        global year
        year = str(current_date.year)
        merge_with_sqft()

#         top5properties,top_5_values = current_top_5_property()
#
#         # ================last_year====================
#         year = str(current_date.year-1)
#         last_year_data = merge_with_sqft()
#         datamerged_top5_last_year = last_year_data.loc[last_year_data['National_tenant'].isin(top5properties)]
#
#         last_year_values = []
#         for prop in top5properties:
#             value = datamerged_top5_last_year[datamerged_top5_last_year['National_tenant'] == prop]['NOI_Persqft'].values[0]
#             last_year_values.append(value)
#
#         try:
#     # ===========percent diff each property=====================
#             percent_diff = []
#             for curre,prev in zip(top_5_values,last_year_values):
#
#                 pe_df = ((curre - prev) / prev) * 100
#                 ok = "{:.2f}".format(pe_df)
#                 percent_diff.append(ok)
#
#
#             # ==============PLOT=====================
#             x_axis = top5properties
#             y_axis = top_5_values
#             graph = PLOT(x_axis,y_axis,percent_diff)
#             final,data_template = create_html_template(graph)
#
#             try:
# # =====================write the DataFrame to a table in the sql database
#                 for index, row in data_template.iterrows():
#                     InsightsMasterId = row['InsightsMasterId']
#                     TemplateId = row['TemplateId']
#                     EmailTOAddress = row['UserEmail']
#                     EmailCCAddress = row['EmailCCAddress']
#                     Subject = row['Subject']
#                     Body = str(final)
#                     SendToId = row['SendToId']
#                     storedProc = "Exec [InsertEmailHistoryManageInsights] @InsightsMasterId = ?, @TemplateId = ?, @EmailTOAddress = ?, @EmailCCAddress = ?, @Subject = ?,@Body = ?,@SendToId = ?"
#                     params = (InsightsMasterId, TemplateId, EmailTOAddress, EmailCCAddress, Subject, Body,SendToId)
#                     connection = sql_connection()
#                     cursor = connection.cursor()
#                     cursor.execute(storedProc, params)
#                     connection.commit()
#
#
#                     message = BasicMessage()
#                     message.subject = Subject
#                     message.html_body = str(final)
#                     message.from_email_address = EmailAddress("rohit.mohite@annet.com")
#                     for to_item in EmailTOAddress.split(','):
#                         message.add_to_email_address(to_item)
#
#                     for cc_item in EmailCCAddress.split(','):
#                         message.add_cc_email_address(cc_item)
#
#                     client = SocketLabsClient(serverId, injectionApiKey)
#                     response = client.send(message)
#                     success_ran()
#
#             except Exception as e:
#                 print("ERROR: " + str(e))
#                 cron_fail()
#         except Exception as e:
#             print("ERROR: " + str(e))
#             cron_fail()
    except Exception as e:
        print(e)
        # sql_conn_fail()



