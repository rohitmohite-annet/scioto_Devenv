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




def persqft_data():
    connection = sql_connection()
    sqft = pd.read_sql("select [PropertyID],[PropertyManager],[PropertyPKID],[Company Description],[Property Status],[Property Type],[Unit Square Feet] from [dbo].[viewPropertyUnitLeaseDetails] where PropertyManager <> '' ",connection)
    connection.close()
    Sqft_data = pd.DataFrame()
    for key, Promanage in enumerate(list(sqft['PropertyManager'].unique())):
        datasq = sqft[sqft['PropertyManager'] == Promanage]
        sqft_sum = datasq['Unit Square Feet'].sum()

        write_to_data = {'National_tenant': Promanage, 'Unit Square Feet': sqft_sum}
        dataframe_to_write = pd.DataFrame([write_to_data], columns=write_to_data.keys())
        Sqft_data = Sqft_data.append(dataframe_to_write, ignore_index=True)
    Sqft_data = Sqft_data.loc[Sqft_data['Unit Square Feet'] > 0]

    return Sqft_data

def fetch_data_NOI():
    connection = sql_connection()
    data = pd.read_sql(
        "select PropertyManager,NetPostingPeriodDate,Amount,FiscalYear,FiscalMonth,L1,L2,L3,L4,L5,L6,L7,L8,L9 from [dbo].[viewIncomeStatement] where PropertyManager <> '' AND FiscalYear = {} AND FiscalMonth in {} and Ledger = 'AA' ".format(
            str(year)[-2:], monthlist), connection)
    connection.close()
    return data


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
    merged_sqft.to_csv('mergesqft_{}'.format(year)+'.csv')
    return merged_sqft


def current_top_5_property():
    current_year_data = merge_with_sqft()
    top5properties = current_year_data.sort_values(by=['NOI_Persqft'], ascending=False)[:5]['National_tenant'].to_list()
    top_5_values = current_year_data.sort_values(by=['NOI_Persqft'], ascending=False)[:5]['NOI_Persqft'].to_list()
    return top5properties,top_5_values


def PLOT(x_axis,y_axis,percent_diff):
    sns.set(rc={'axes.facecolor': '#f6f6f6', 'figure.facecolor': '#f6f6f6'})
    ax = sns.barplot(x=x_axis, y=y_axis, joinstyle='bevel')
    ax.figure.set_size_inches(10, 6)
    # ax.set_ylabel('NOI Amount per sq.ft ', size=23,weight='bold')

    def currency(x, pos):
        """The two args are the value and tick position"""
        if x >= 1e6:
            s = '${:1.1f}M'.format(x * 1e-6)
        elif x >= 1e3:
            s = '${:1.0f}K'.format(x * 1e-3)
        else:
            s = '${:1.0f}'.format(x)
        return s

    def change_width(ax, new_value):
        for patch in ax.patches:
            current_width = patch.get_width()
            diff = current_width - new_value

            # we change the bar width
            patch.set_width(new_value)

            # we recenter the bar
            patch.set_x(patch.get_x() + diff * .5)

    change_width(ax, 0.6)

    new_patches = []
    mut_Aspect = max(y_axis)

    for patch in reversed(ax.patches):
        bb = patch.get_bbox()

        p_bbox = FancyBboxPatch((bb.xmin, bb.ymin),
                                abs(bb.width), abs(bb.height),
                                boxstyle="round, pad=0.030,rounding_size = 0.045",
                                ec="none", fc='#4298af',
                                mutation_aspect=mut_Aspect
                                )
        patch.remove()
        new_patches.append(p_bbox)

    for patch in new_patches:
        ax.add_patch(patch)

    sns.despine(top=True, right=True)

    ax.tick_params(axis=u'both', which=u'both', length=0,pad=6)
    plt.tick_params(labelsize=14.5,pad=6)
    for index, value in enumerate(y_axis):
        plt.text(index, value * 1.02, '$' + str(value), fontsize=17, ha='center', va='top',
                 color='white', weight='bold')

    plt.ticklabel_format(style='plain', axis='y')
    plt.rcParams["font.family"] = "Open Sans"

    def add_value_labels(ax, spacing=16):
        # For each bar: Place a label
        for perdif, rect in zip(percent_diff[::-1], ax.patches):

            # Get X and Y placement of label from rect.
            y_value = rect.get_height()
            x_value = rect.get_x() + rect.get_width() / 2

            # Number of points between bar and label. Change to your liking.
            space = spacing
            # Vertical alignment for positive values
            va = 'bottom'

            # If value of bar is negative: Place label below bar
            if y_value < 0:
                # Invert space to place label below
                space *= -1
                # Vertically align label at top
                va = 'top'

            # Use Y value as label and format number with one decimal place
            #         label = "{:.1f}".format(y_value)
            if float(perdif) > 0:
                label = "\u21E7 +{}%".format(perdif)
            elif float(perdif) < 0:
                jk = float(perdif)
                neg_handle = abs(jk)
                label = "\u21E9 -{}%".format(neg_handle)
            else:
                label = "{}%".format(perdif)

            # Create annotation
            ax.annotate(
                label,  # Use `label` as label
                (x_value, y_value),  # Place label at end of the bar
                xytext=(0, space),  # Vertically shift label by `space`
                textcoords="offset points",  # Interpret `xytext` as offset in points
                fontsize = 17,
                weight='bold',
                ha='center',  # Horizontally center label
                va=va)  # Vertically align label differently for
            # positive and negative values.

    # Call the function above. All the magic happens there.
    add_value_labels(ax)
    ax.yaxis.set_major_formatter(currency)
    for label in (ax.get_xticklabels() + ax.get_yticklabels()):
        label.set_fontsize(17)
        label.set_fontweight('bold')
    ax.spines['left'].set_color('black')
    ax.spines['bottom'].set_color('black')
    plt.tight_layout()

    # plt.savefig('latest_5.png')
    image_stream = BytesIO()
    plt.savefig(image_stream)
    image_stream.seek(0)
    my_base64_jpgData = base64.b64encode(image_stream.read())
    graph = my_base64_jpgData.decode("utf-8")
    return graph


def create_html_template(graph):
    insight_title = 'NET OPERATING INCOME : PSF'
    insight_message = 'Top 5 National Tenants'
    insight_graph = graph
    connection = sql_connection()
    data = pd.read_sql("select * from [dbo].[viewAllManageInsights] where InsightsMasterId = 14", connection)
    connection.close()

    Html_Template = data.Body[0]
    final = Html_Template.format(insight_title=insight_title, insight_message=insight_message,
                                 insight_graph=insight_graph)
    return final,data


if __name__=='__main__':
    try:
        global year, month,monthlist
        months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        todaysdate = date.today()
        if (todaysdate.day) < 21:
            if (todaysdate.month == 1):
                month = 11
                year = todaysdate.year - 1
            elif todaysdate.month == 2:
                month = 12
                year = todaysdate.year - 1
            else:
                month = todaysdate.month - 2
                year = todaysdate.year
        else:
            if (todaysdate.month == 1):
                month = 12
                year = todaysdate.year - 1
            else:
                month = todaysdate.month - 1
                year = todaysdate.year

        monthlist = tuple(months[:month])

        year = str(year)
        top5properties,top_5_values = current_top_5_property()

        # ================last_year====================
        year = str(int(year)-1)
        last_year_data = merge_with_sqft()
        datamerged_top5_last_year = last_year_data.loc[last_year_data['National_tenant'].isin(top5properties)]

        last_year_values = []
        for prop in top5properties:
            value = datamerged_top5_last_year[datamerged_top5_last_year['National_tenant'] == prop]['NOI_Persqft'].values[0]
            last_year_values.append(value)
        print(top_5_values,top5properties,last_year_values)
        try:
    # ===========percent diff each property=====================
            percent_diff = []
            for curre,prev in zip(top_5_values,last_year_values):

                pe_df = ((curre - prev) / prev) * 100
                ok = "{:.2f}".format(pe_df)
                percent_diff.append(ok)


            # ==============PLOT=====================
            x_axis = top5properties
            y_axis = top_5_values
            graph = PLOT(x_axis,y_axis,percent_diff)
            final,data_template = create_html_template(graph)

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
            print("ERROR: " + str(e))
            cron_fail()
    except Exception as e:
        sql_conn_fail()



