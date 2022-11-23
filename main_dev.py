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
