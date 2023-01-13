import matplotlib.pyplot as plt
import warnings
import logging
import calendar
warnings.filterwarnings("ignore")
from matplotlib.patches import FancyBboxPatch
import seaborn as sns
from datetime import datetime, timedelta,date
import pandas as pd
import base64
from io import BytesIO
from emailto_send import *
import pyodbc
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.patches as mpatches
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

def fetch_data_NOI():
    connection = sql_connection()
    months_list = tuple(months)
    data = pd.read_sql("select  * from [dbo].[viewIncomeStatement] where PropertyManager <> ''  and  FiscalYear = {} and FiscalMonth in {} and Ledger = 'AA' ".format(str(year)[-2:],months_list), connection)
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

def current_bottom_5_property():
    current_year_data = merge_with_sqft()
    current_year_data = current_year_data[current_year_data['NOI_Persqft'] > 0]
    top5properties = current_year_data.sort_values(by=['NOI_Persqft'], ascending=True)[:5]['National_tenant'].to_list()
    top_5_values = current_year_data.sort_values(by=['NOI_Persqft'], ascending=True)[:5]['NOI_Persqft'].to_list()
    return top5properties,top_5_values


def PLOT1(data_2022,NT_2022,data_2021,NT_2021,current_year,previous_year):
    width = 0.4
    x = np.arange(1, 6)
    sns.set(rc={'axes.facecolor': '#f6f6f6', 'figure.facecolor': '#f6f6f6'})
    fig, ax = plt.subplots(figsize=(15, 8))

    ax.bar(np.array([-0.2, 0.8, 1.8, 2.8, 3.8]), data_2022, width, tick_label=NT_2022, color='#4298af', label=current_year)
    ax.bar(np.array([0.2, 1.2, 2.2, 3.2, 4.2]), data_2021, width, tick_label=NT_2022, color='#93a9d0', label=previous_year)

    plt.xticks(np.array([-0.2, 0.8, 1.8, 2.8, 3.8, 0.2, 1.2, 2.2, 3.2, 4.2]),
               np.array(np.concatenate([NT_2022, NT_2021])))

    ax.margins(x=0.06)
    plt.legend(bbox_to_anchor=(0.93, 1), loc='upper left', borderaxespad=0, prop={'size': 23, 'weight': 'bold'})

    def currency(x, pos):
        """The two args are the value and tick position"""
        if x >= 1e6:
            s = '${:1.1f}M'.format(x * 1e-6)
        elif x >= 1e3:
            s = '${:1.0f}K'.format(x * 1e-3)
        else:
            s = '${:1.0f}'.format(x)
        return s

    ax.yaxis.set_major_formatter(currency)

    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)

    ax.set_xticks(x - 0.1001, minor=True)

    new_patches = []
    for patch in reversed(ax.patches):
        bb = patch.get_bbox()
        color = patch.get_facecolor()
        p_bbox = FancyBboxPatch((bb.xmin, bb.ymin),
                                abs(bb.width), abs(bb.height),
                                boxstyle="round,pad=0.0020,rounding_size=0.017",
                                ec="none", fc=color,
                                mutation_aspect=max(max(data_2022), max(data_2021))
                                )
        patch.remove()
        new_patches.append(p_bbox)
    for patch in new_patches:
        ax.add_patch(patch)

    a = list(zip(data_2022, data_2021))
    coordinates1 = list(np.concatenate(a).flat)

    def currency1(x):
        """The two args are the value and tick position"""
        s = '${:1.0f}'.format(x)
        #     s = '$' + f'{x:,}'
        if x >= 1e6:
            s = '${:1.2f}M'.format(x * 1e-6)
        elif x >= 1e3:
            s = '${:1.2f}K'.format(x * 1e-3)
        else:
            s = '$' + f'{x:,}'
        return s

    xpos = []
    for rect in ax.patches:
        xpos.append(rect.get_x())
    xpos = sorted(set(xpos))

    for i in range(len(coordinates1)):
        ax.annotate(
            currency1(coordinates1[i]),  # Use `label` as label
            (xpos[i] + 0.43 / 2, coordinates1[i]),  # Place label at end of the bar
            xytext=(0, 5),  # Vertically shift label by `space`
            textcoords="offset points",  # Interpret `xytext` as offset in points
            fontsize=16,
            weight='bold',
            ha='center',  # Horizontally center label
            va='bottom')

    for label in (ax.get_xticklabels() + ax.get_yticklabels()):
        label.set_fontsize(17)
        label.set_fontweight('bold')

    ax.spines['left'].set_color('black')
    ax.spines['bottom'].set_color('black')
    plt.xticks(rotation=45)
    plt.tight_layout()
    # plt.show()
    image_stream = BytesIO()
    plt.savefig(image_stream)
    image_stream.seek(0)
    my_base64_jpgData = base64.b64encode(image_stream.read())
    graph = my_base64_jpgData.decode("utf-8")
    return graph

def create_html_template(graph):
    insight_title = 'NET OPERATING INCOME : PSF'
    insight_message = 'Bottom 5 National Tenants for YOY'
    insight_graph = graph
    connection = sql_connection()
    data = pd.read_sql("select * from [dbo].[viewAllManageInsights] where InsightsMasterId = 18", connection)
    connection.close()

    Html_Template = data.Body[0]
    final = Html_Template.format(insight_title=insight_title, insight_message=insight_message,
                                 insight_graph=insight_graph)
    return final,data


if __name__=='__main__':
    try:
        global year, months
        todaysdate = date.today()
        if (todaysdate.day) < 21:

            if (todaysdate.month == 1):
                months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11']
                year = todaysdate.year - 1
            elif todaysdate.month == 2:
                months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
                year = todaysdate.year - 1
            else:
                months = []
                year = ''
        else:
            if ((todaysdate.month == 1) or (todaysdate.month == 2)):
                months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
                year = todaysdate.year - 1
            else:
                months = []
                year = ''


        print(year,months)
        top5properties,top_5_values = current_bottom_5_property()
        current_year = year
        print(top5properties,top_5_values)


        year = str(current_year - 1)
        previous_year = year
        top5properties_last, top_5_values_last = current_bottom_5_property()

        graph = PLOT1(top_5_values,top5properties,top_5_values_last,top5properties_last,current_year,previous_year)
        print(top5properties_last, top_5_values_last)

#
        final,data_template = create_html_template(graph)
        print(final)


        try:
# =====================write the DataFrame to a table in the sql database
            for index, row in data_template.iterrows():
                InsightsMasterId = row['InsightsMasterId']
                TemplateId = row['TemplateId']
                EmailTOAddress = row['UserEmail']
                EmailCCAddress = row['EmailCCAddress']
                Subject = row['Subject']
                Body = str(final)
                SendToId = row['SendToId']
                storedProc = "Exec [InsertEmailHistoryManageInsights] @InsightsMasterId = ?, @TemplateId = ?, @EmailTOAddress = ?, @EmailCCAddress = ?, @Subject = ?,@Body = ?,@SendToId = ?"
                params = (InsightsMasterId, TemplateId, EmailTOAddress, EmailCCAddress, Subject, Body,SendToId)
                connection = sql_connection()
                cursor = connection.cursor()
                cursor.execute(storedProc, params)
                connection.commit()

                # send mail to users
                message = BasicMessage()
                message.subject = Subject
                message.html_body = str(final)
                message.from_email_address = EmailAddress("notify@4seeanalytics.com")
                for to_item in EmailTOAddress.split(','):
                    message.add_to_email_address(to_item)

                for cc_item in EmailCCAddress.split(','):
                    message.add_cc_email_address(cc_item)

                client = SocketLabsClient(serverId, injectionApiKey)
                response = client.send(message)
                success_ran()

        except Exception as e:
            print("ERROR: " + str(e))
            cron_fail()

    except Exception as e:
        print(e)
        # sql_conn_fail()



