import pandas as pd




def extract(csv_path,parquet_path):
    
    try:
        df1 = pd.read_csv(csv_path)
        df2 = pd.read_parquet(parquet_path)
        df = pd.concat([df1, df2], ignore_index=True)
        print('¡successful data combination!')
        return df
        
    except Exception as error:
        print(f'Error: {error}')
    

def get_unique_values(df):

    starships= df['starships'].unique()
    df_starships = pd.DataFrame(starships, columns= ['starships'] )
    return df_starships

def record_counts(df):

    df_record = df.groupby(['skin_color', 'eye_color']).size().reset_index(name='count')
    return df_record


def get_duplicate_names(df):

    duplicate_names = df.groupby('name').size().reset_index(name='count')
    duplicate_names = duplicate_names[duplicate_names['count']>1]

    return duplicate_names


def load_to_db(df, sql_connection,table_name):

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)