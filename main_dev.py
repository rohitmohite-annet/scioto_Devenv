import pandas as pd
import pyodbc
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
print('sqft',sqft.shape)


def join_data(data,sqft,year):
    data = data[data['FiscalYear'] == int(year[-2:])]
    merged_sqft = data.merge(sqft, on='PropertyID', how='left')
    print(data.shape)
    print('from func',merged_sqft['FiscalYear'].value_counts())
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
        #     print(NOI_amount,Revenue_amount,Expenses_amount)
        #     print('//')
        #     print(persqft,NOI_persq,Revenue_persq,Expenses_persq)

        towrite = {'property_ID': property_ID, 'propertyname': i, 'SquareFootage': persqft,
                   'NOI_amount': NOI_amount, 'Revenue_amount': Revenue_amount, 'Expenses_amount': Expenses_amount,
                   'NOI_persq': NOI_persq, 'Revenue_persq': Revenue_persq, 'Expenses_persq': Expenses_persq}

        dataframe_to_write = pd.DataFrame([towrite], columns=towrite.keys())

        Final_dataframe = Final_dataframe.append(dataframe_to_write, ignore_index=True)
        print(i)
        counter += 1
        if counter ==10:
            break
    return Final_dataframe
if __name__ == "__main__":
    data_2022 = join_data(data, sqft, '2022')
    final_sq_2022 =  persq_dataframe(data_2022)
    print(final_sq_2022.head())

