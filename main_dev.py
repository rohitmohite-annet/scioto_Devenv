import pandas as pd
data = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\viewIncomeStatement.csv')
print(data.shape)
sqft = pd.read_csv('C:\\Rohit_Data\\WORK\\Scioto_Work\\Dev_Environment\\Data\\Viewpropertyunitlease.csv',usecols=['PropertyID','Property Description','Unit Square Feet'])
sqft.drop_duplicates(subset="PropertyID",inplace=True)
print(sqft.shape)
print('Unitsqft --->',sqft['Unit Square Feet'].isnull().sum())