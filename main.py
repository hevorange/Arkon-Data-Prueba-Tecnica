import etl_functions
import sqlite3


csv_path = './data/Data1.csv'
parquet_path = './data/data2.parquet'
db_name = "arkon.db"
table_name ="person"

def run():

    df = etl_functions.extract(csv_path, parquet_path)
    # df_starships_unique = etl_functions.get_unique_values(df)
    # print(df_starships_unique)

    # print(etl_functions.record_counts(df))

    # print(etl_functions.get_duplicate_names(df))

    sql_connection = sqlite3.connect(db_name)
    etl_functions.load_to_db(df, sql_connection, table_name)


    """ PASO 5 -enerar una query de SQL que muestre la siguiente información, los nombres que tengan un
        height arriba de 180 pero menor a 190, que cumplan la condición de ser male y el hair_color
        sea cualquiera menos “none”

    query_statement = f"SELECT * FROM {table_name} WHERE height > 180 AND height < 190 AND sex='male' AND hair_color!='none'"
    etl_functions.run_query(query_statement,sql_connection)
    
    """

    



if __name__=='__main__':
    run()