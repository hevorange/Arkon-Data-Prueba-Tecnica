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
