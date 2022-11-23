import pyodbc
import matplotlib.pyplot as plt
from pandas.tseries.offsets import DateOffset
import warnings
warnings.filterwarnings("ignore")
from matplotlib.patches import FancyBboxPatch
import seaborn as sns
from datetime import datetime, timedelta,date
import pandas as pd

def fetch_data():
    try:
        server = 'epsql-srv-scioto-4see.database.windows.net'
        database = 'qasciotodb'
        username = 'sciotosqladmin'
        password = 'Ret$nQ2stkl21'
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        data = pd.read_sql("select * from [dbo].[viewIncomeStatement] where Ledger = 'AA' AND PropertyStatus='Active'",cnxn)
        data['NetPostingPeriod'] = pd.to_datetime(data['NetPostingPeriod'])
        return data
    except Exception as e:
            return 'From reading data'+ str(e)


data = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\viewIncomeStatement.csv')
data['NetPostingPeriod'] = pd.to_datetime(data['NetPostingPeriod'])
data = data[(data['Ledger'] == 'AA')]

sqft = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\Viewpropertyunitlease.csv',usecols=['PropertyID','Property Description','Unit Square Feet'])
sqft.drop_duplicates(subset="PropertyID",inplace=True)



def join_data(data,sqft,year):
    data = data[data['FiscalYear'] == int(year[-2:])]
    merged_sqft = data.merge(sqft, on='PropertyID', how='left')
    print('merged_sqft.shape',merged_sqft.shape)
    print('data.shape',data.shape)
    return merged_sqft



def persq_dataframe(merged_sqft):
    dict_new = dict()
    Final_dataframe = pd.DataFrame()

    def cal_NOI(df):
        Revenue = df[df['L3'] == 'REVENUE']['Amount'].sum()
        Expenses = df[df['L3'] == 'OPERATING EXPENSES']['Amount'].sum()
        diff = Revenue - Expenses
        return round(diff, 2), round(Revenue, 2), round(Expenses, 2)

    def persq_cal(amount, persqft):
        if persqft == 0:
            return 0
        else:
            return round(amount / persqft, 2)

    counter = 0
    for i in list(merged_sqft['Property Name'].unique()):
        proprty_data = merged_sqft[merged_sqft['Property Name'] == i]
        property_ID = proprty_data['PropertyID'].iloc[0]
        persqft = proprty_data['Unit Square Feet'].iloc[0]
        NOI_amount, Revenue_amount, Expenses_amount = cal_NOI(proprty_data)

        NOI_persq = persq_cal(NOI_amount, persqft)
        Revenue_persq = persq_cal(Revenue_amount, persqft)
        Expenses_persq = persq_cal(Expenses_amount, persqft)

        towrite = {'property_ID': property_ID, 'propertyname': i, 'SquareFootage': persqft,
                   'NOI_amount': NOI_amount, 'Revenue_amount': Revenue_amount, 'Expenses_amount': Expenses_amount,
                   'NOI_persq': NOI_persq, 'Revenue_persq': Revenue_persq, 'Expenses_persq': Expenses_persq}

        dataframe_to_write = pd.DataFrame([towrite], columns=towrite.keys())
        Final_dataframe = Final_dataframe.append(dataframe_to_write, ignore_index=True)
        counter += 1
    return Final_dataframe

def top_5plot():
    data_2022 = join_data(data, sqft, '2022')
    final_sq_2022 = persq_dataframe(data_2022)
    print(final_sq_2022.shape)
    x_axis = final_sq_2022.sort_values(by=['NOI_persq'],ascending = False)[:5]['propertyname'].to_list()
    y_axis = final_sq_2022.sort_values(by=['NOI_persq'],ascending = False)[:5]['NOI_persq'].to_list()
    sns.set(rc={'axes.facecolor': '#EDF3D5', 'figure.facecolor': '#EDF3D5'})
    ax = sns.barplot(x=x_axis, y=y_axis, joinstyle='bevel')
    ax.figure.set_size_inches(10, 6)
    ax.set_ylabel('Amount Operating Expense per sq.ft ', size=15)



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
        # print(bb.xmin, bb.ymin,abs(bb.width), abs(bb.height))
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
        plt.text(index, value // 1.02, '$' + str(value), fontsize=15, ha='center', va='top',
                 color='white', weight='bold')

    plt.ticklabel_format(style='plain', axis='y')
    plt.rcParams["font.family"] = "Open Sans"
    plt.tight_layout()
    plt.savefig("top5_exp_Persqft.png")
    plt.show()

def comp_YTM():
    return 'd'
if __name__ == "__main__":
    top_5plot()


