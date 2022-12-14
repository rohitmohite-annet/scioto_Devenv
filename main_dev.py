import pyodbc
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
from matplotlib.patches import FancyBboxPatch
import seaborn as sns
from datetime import datetime, timedelta,date
import pandas as pd



def sql_connection():
    server = 'epsql-srv-scioto-4see.database.windows.net'
    database = 'qasciotodb'
    username = 'sciotosqladmin'
    password = 'Ret$nQ2stkl21'
    cnxn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn


connection1 = sql_connection()
data = pd.read_sql("select * from [dbo].[viewIncomeStatement] where Ledger = 'AA' AND PropertyStatus='Active' AND L3 = 'OPERATING EXPENSES'",connection1)
data['NetPostingPeriod'] = pd.to_datetime(data['NetPostingPeriod'])
data["FiscalYear"] = pd.to_numeric(data["FiscalYear"])
connection1.close()
#

connection = sql_connection()
sqft = pd.read_sql("select * from [dbo].[viewPropertyUnitLeaseDetails]",connection)
sqft = sqft.loc[sqft['Unit Square Feet'] > 0]
sqft.drop_duplicates(subset="PropertyID",inplace=True)
connection.close()

#
# ============= Read from Files================
# data = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\viewIncomeStatement.csv')
# data['NetPostingPeriod'] = pd.to_datetime(data['NetPostingPeriod'])
# data = data[(data['Ledger'] == 'AA')]
#
# sqft = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\Viewpropertyunitlease.csv',usecols=['PropertyID','Property Description','Unit Square Feet'])
# sqft.drop_duplicates(subset="PropertyID",inplace=True)
#


def join_data(data,sqft,year):
    data = data[data['FiscalYear'] == int(year[-2:])]
    merged_sqft = data.merge(sqft, on='PropertyID', how='left')

    return merged_sqft

def getdata_range_YTM():
    current_date = date.today()
    last_day_of_prev_month = pd.to_datetime(date(current_date.year, current_date.month - 1, 20))
    current_year = date.today().year
    current_first_date = date(date.today().year, 1, 1)

    previous_date = last_day_of_prev_month + pd.DateOffset(years=-1)
    previous_year = current_year - 1
    previous_first_date = date(previous_date.year, 1, 1)
    return current_first_date,last_day_of_prev_month,previous_first_date,previous_date

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
    final_sq_2022.to_csv('persq2022.csv')
    print(final_sq_2022.head())


    # ===============Finding persq ft NOI==========================
    top5properties = final_sq_2022.sort_values(by=['NOI_persq'], ascending=False)[:5]['propertyname'].to_list()

    data_2021 = join_data(data, sqft, '2021')
    datamerged_top52021 = data_2021.loc[data_2021['Property Name'].isin(top5properties)]

    final_sq_2021 = persq_dataframe(datamerged_top52021)
    final_sq_2021.to_csv('2021persqft.csv')
    last_year_values = []
    for prop in top5properties:
        value = final_sq_2021[final_sq_2021['propertyname'] == prop]['NOI_persq'].values[0]
        last_year_values.append(value)


    # ===========percent diff each property=====================
    current_value = final_sq_2022.sort_values(by=['NOI_persq'], ascending=False)[:5]['NOI_persq'].to_list()
    percent_diff = []
    for curre,prev in zip(current_value,last_year_values):
        print(curre,prev)
        pe_df = ((curre - prev) / prev) * 100
        ok = "{:.2f}".format(pe_df)
        percent_diff.append(ok)
    print(percent_diff)




    x_axis = final_sq_2022.sort_values(by=['NOI_persq'],ascending = False)[:5]['propertyname'].to_list()
    y_axis = final_sq_2022.sort_values(by=['NOI_persq'],ascending = False)[:5]['NOI_persq'].to_list()

    sns.set(rc={'axes.facecolor': '#EDF3D5', 'figure.facecolor': '#EDF3D5'})
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
        plt.text(index, value * 1.02, '$' + str(value), fontsize=17, ha='center', va='top',
                 color='white', weight='bold')

    plt.ticklabel_format(style='plain', axis='y')
    plt.rcParams["font.family"] = "Open Sans"

    def add_value_labels(ax, spacing=16):


        # For each bar: Place a label
        for perdif,rect in zip(percent_diff[::-1],ax.patches):

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
                jk  = float(perdif)
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
                ha='center',  # Horizontally center label
                va=va)  # Vertically align label differently for
            # positive and negative values.

    # Call the function above. All the magic happens there.
    add_value_labels(ax)
    for label in (ax.get_xticklabels() + ax.get_yticklabels()):
        label.set_fontsize(17)
    ax.yaxis.set_major_formatter(currency)
    plt.tight_layout()
    plt.savefig("top5_exp_Persqft.png")


def comp_YTM():
    current_first_date,last_day_of_prev_month,previous_first_date,previous_date = getdata_range_YTM()
    current_year_data = data[(data['NetPostingPeriod'] >= pd.to_datetime(current_first_date)) &
                             (data['NetPostingPeriod'] <= pd.to_datetime(last_day_of_prev_month))]

    previous_year_data = data[(data['NetPostingPeriod'] >= pd.to_datetime(previous_first_date)) &
                              (data['NetPostingPeriod'] <= pd.to_datetime(previous_date))]

    # =======merge sq ft data========
    last_year_data_sq_merge = previous_year_data.merge(sqft, on='PropertyID', how='left')
    current_year_data_sq_merge = current_year_data.merge(sqft, on='PropertyID', how='left')


    # =======Per sq ft calculation each yaer========
    last_year_persq = persq_dataframe(last_year_data_sq_merge)
    this_year_persq = persq_dataframe(current_year_data_sq_merge)

    last_year_persq_amount = last_year_persq['NOI_persq'].sum()
    current_year_persq_amount = this_year_persq['NOI_persq'].sum()
    return str(previous_first_date.year) ,last_year_persq_amount,str(current_first_date.year),current_year_persq_amount

# def Expensecomp_persqft_Plot():
#     lastyear_name,last_month_Expense,currentyear_name,current_month_Expense = comp_YTM()
#     print(lastyear_name,last_month_Expense,currentyear_name,current_month_Expense)
#
#     x_ax = [str(lastyear_name), str(currentyear_name)]
#     y_ax = [abs(last_month_Expense), abs(current_month_Expense)]
#     sns.set(rc={'axes.facecolor': '#EDF3D5', 'figure.facecolor': '#EDF3D5'})
#     ax1 = sns.barplot(x=x_ax, y=y_ax, joinstyle='bevel')
#     ax1.set_ylabel('Amount Operating Expense per sq.ft ', size=15)
#     ax1.figure.set_size_inches(8, 7.3)
#
#     def currency(x, pos):
#         """The two args are the value and tick position"""
#         if x >= 1e6:
#             s = '${:1.1f}M'.format(x * 1e-6)
#         elif x >= 1e3:
#             s = '${:1.0f}K'.format(x * 1e-3)
#         else:
#             s = '${:1.0f}'.format(x)
#         return s
#
#     def change_width1(ax1, new_value):
#         for patch in ax1.patches:
#             current_width = patch.get_width()
#             diff = current_width - new_value
#
#             # we change the bar width
#             patch.set_width(new_value)
#
#             # we recenter the bar
#             patch.set_x(patch.get_x() + diff * .5)
#
#     change_width1(ax1, 0.40)
#
#     new_patches1 = []
#     mut_Aspect = max(current_month_Expense, last_month_Expense)
#
#     for patch in reversed(ax1.patches):
#
#         bb = patch.get_bbox()
#         p_bbox = FancyBboxPatch((bb.xmin, bb.ymin),
#                                 abs(bb.width), abs(bb.height),
#                                 boxstyle="round, pad=0.030,rounding_size = 0.030",
#                                 ec="none", fc='#728137',
#                                 mutation_aspect=mut_Aspect
#                                 )
#
#         patch.remove()
#         new_patches1.append(p_bbox)
#
#     for patch in new_patches1:
#         ax1.add_patch(patch)
#     print('comparsion', new_patches1)
#     sns.despine(top=True, right=True)
#
#     ax1.tick_params(axis=u'both', which=u'both', length=0)
#     if abs(current_month_Expense) > 1000000 or abs(last_month_Expense) > 1000000:
#         for index, value in enumerate(y_ax):
#             plt.text(index, value // 1.02, '$' + str(round(value / 1000000, 2)) + ' M', fontfamily='Open Sans',
#                      fontsize=20, ha='center', va='top',
#                      color='white', weight='bold')
#     else:
#         for index, value in enumerate(y_ax):
#             plt.text(index, value // 1.02, '$' + str(round(value / 1000, 2)) + ' K', fontsize=20, ha='center',
#                      va='top',
#                      color='white', weight='bold')
#     plt.tick_params(labelsize=18)
#     plt.ticklabel_format(style='plain', axis='y')
#     plt.rcParams["font.family"] = "Open Sans"
#     ax1.yaxis.set_major_formatter(currency)
#     plt.tight_layout()
#     plt.savefig('YTM_COMPARISON.png')



if __name__ == "__main__":
    top_5plot()
    a,b,c,d= comp_YTM()
    print(a,b,c,d)


