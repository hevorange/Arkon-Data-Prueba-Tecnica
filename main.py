import etl_functions


csv_path = './data/Data1.csv'
parquet_path = './data/data2.parquet'

df = etl_functions.extract(csv_path, parquet_path)

# df_starships_unique = etl_functions.get_unique_values(df)
# print(df_starships_unique)

# print(etl_functions.record_counts(df))

# print(etl_functions.get_duplicate_names(df))