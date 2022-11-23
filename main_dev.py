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

print(data.shape)
sqft = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\Viewpropertyunitlease.csv',usecols=['PropertyID','Property Description','Unit Square Feet'])
sqft.drop_duplicates(subset="PropertyID",inplace=True)
print(sqft.head())
print('Unitsqft --->',sqft['Unit Square Feet'].isnull().sum())

merged_sqft = data.merge(sqft, on='PropertyID', how='left')
print('merge',merged_sqft.shape)
print('data',data.shape)

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
        print(i)
        counter += 1
        if counter ==3:
            break

if __name__ == "__main__":
    persq_dataframe(merged_sqft)