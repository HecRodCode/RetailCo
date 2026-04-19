import pandas as pd

df = pd.read_csv('../data/Amazon Sale Report.csv', low_memory=False)

print("--- Dimensions ---")
print(df.shape)

print("--- Data Types ---")
print(df.dtypes)

print("--- Null data percentage ---")
print(df.isnull().sum() / len(df) * 100)

print("--- Incorrect data types ---")
print(df[['Date', 'Amount', 'Qty']].dtypes)

print("--- Duplicated data ---")
print(df.duplicated().sum())

print("--- Summary of the most important columns ---")
important_columns = ['Qty', 'Amount']
print(df[important_columns].describe())