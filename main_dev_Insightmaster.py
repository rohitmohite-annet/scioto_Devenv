import pyodbc
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
from matplotlib.patches import FancyBboxPatch
import seaborn as sns
from datetime import datetime, timedelta,date
import pandas as pd
import plotly.graph_objects as go
import math


def sql_connection():
    server = 'epsql-srv-scioto-4see.database.windows.net'
    database = 'qasciotodb'
    username = 'sciotosqladmin'
    password = 'Ret$nQ2stkl21'
    cnxn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn

connection = sql_connection()
sqft = pd.read_sql("select [PropertyID],[PropertyPKID],[Company Number],[Company Description],[Property Status],[Property Type],[Unit Square Feet] from [dbo].[viewPropertyUnitLeaseDetails]",connection)
sqft = sqft.loc[sqft['Unit Square Feet'] > 0]
sqft.drop_duplicates(subset="PropertyID",inplace=True)
connection.close()





months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
current_date = datetime.now()

if int(current_date.strftime("%d")) > 20:
    current_month = datetime.now().month - 1
    monthlist = tuple(months[:current_month])
else:
    current_month = datetime.now().month - 2
    monthlist = tuple(months[:current_month])


def fetch_data_NOI(year):
    connection = sql_connection()
    data = pd.read_sql("select * from [dbo].[viewInsightsData] where KPI = 'Net Operating Income' AND PostYear = {} AND PostMonth in {}".format(year,monthlist), connection)
    connection.close()
    return data

def calcluate(year):
    get_data = fetch_data_NOI(year)
    Final_dataframe = pd.DataFrame()
    for ind,i in  enumerate(list(get_data['AttributeName'].unique())):
        proprty_data = get_data[get_data['AttributeName'] == i]
        NOI_sum = proprty_data['AttributeValue'].sum()
        KPI = proprty_data['KPI'].iloc[0]
        Type= proprty_data['Type'].iloc[0]
        year = proprty_data['PostYear'].iloc[0]
        AttributeCode = proprty_data['AttributeCode'].iloc[0]
        towrite = {'Index' : ind ,'Company Number' : AttributeCode,'propertyname': i,'KPI' : KPI,'Type' :Type ,'YEAR':year ,'NOI_amount': NOI_sum}
        dataframe_to_write = pd.DataFrame([towrite], columns=towrite.keys())
        Final_dataframe = Final_dataframe.append(dataframe_to_write, ignore_index=True)
    return Final_dataframe

def merge_with_sqft():
    check = calcluate(year)
    merged_sqft = check.merge(sqft, on='Company Number', how='left')
    merged_sqft['NOI_Persqft'] = round((merged_sqft['NOI_amount']/merged_sqft['Unit Square Feet']),2)
    merged_sqft = merged_sqft[['Index','Company Number','propertyname','KPI','Type','YEAR','NOI_amount','Unit Square Feet','NOI_Persqft']]
    return merged_sqft




def current_top_5_property():
    current_year_data = merge_with_sqft()
    current_total_sqft = current_year_data['NOI_Persqft'].sum()
    top5properties = current_year_data.sort_values(by=['NOI_Persqft'], ascending=False)[:5]['propertyname'].to_list()
    top_5_values = current_year_data.sort_values(by=['NOI_Persqft'], ascending=False)[:5]['NOI_Persqft'].to_list()
    print(top5properties,top_5_values,current_total_sqft)
    return top5properties,top_5_values,current_total_sqft


def PLOT(xaxis_,y_axis,percent_diff):

    sns.set(rc={'axes.facecolor': '#f6f6f6', 'figure.facecolor': '#f6f6f6'})
    ax = sns.barplot(x=x_axis, y=y_axis, joinstyle='bevel')
    ax.figure.set_size_inches(10, 6)
    ax.set_ylabel('NOI Amount per sq.ft ', size=15)

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
                                ec="none", fc='#728137',
                                mutation_aspect=mut_Aspect
                                )
        patch.remove()
        new_patches.append(p_bbox)

    for patch in new_patches:
        ax.add_patch(patch)

    sns.despine(top=True, right=True)

    ax.tick_params(axis=u'both', which=u'both', length=0)
    for index, value in enumerate(y_axis):
        plt.text(index, value * 1.02, '$' + str(value), fontsize=15, ha='center', va='top',
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
                ha='center',  # Horizontally center label
                va=va)  # Vertically align label differently for
            # positive and negative values.

    # Call the function above. All the magic happens there.
    add_value_labels(ax)
    ax.grid(False)
    ax.yaxis.set_major_formatter(currency)
    plt.tight_layout()
    plt.savefig('latest_2.png')


if __name__=='__main__':
    try:
        year = str(current_date.year)
        top5properties,top_5_values,current_total_sqft = current_top_5_property()

        # ================last_year====================
        year = str(current_date.year-1)
        last_year_data = merge_with_sqft()
        datamerged_top5_last_year = last_year_data.loc[last_year_data['propertyname'].isin(top5properties)]

        last_year_values = []
        for prop in top5properties:
            value = datamerged_top5_last_year[datamerged_top5_last_year['propertyname'] == prop]['NOI_Persqft'].values[0]
            last_year_values.append(value)


        # ===========percent diff each property=====================
        percent_diff = []
        for curre,prev in zip(top_5_values,last_year_values):
            print(curre,prev)
            pe_df = ((curre - prev) / prev) * 100
            ok = "{:.2f}".format(pe_df)
            percent_diff.append(ok)
        print(percent_diff)


        # ==============PLOT=====================
        x_axis = top5properties
        y_axis = top_5_values
        PLOT(x_axis,y_axis,percent_diff)

    except Exception as e:
        print(e)


