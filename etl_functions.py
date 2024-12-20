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
    
    
    
    