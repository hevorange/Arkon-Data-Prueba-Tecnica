import etl_functions


csv_path = './data/Data1.csv'
parquet_path = './data/data2.parquet'

df = etl_functions.extract(csv_path, parquet_path)
print(df)
