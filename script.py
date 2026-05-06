import os
import pandas as pd
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename='/Users/sakshamkapoor/Documents/Data/logs/ingestion_db.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function ingests the data into database'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

path = '/Users/sakshamkapoor/Documents/analysis_project/DA-PROJECT2/data'

def Load_raw_data():
    '''This function loads the raw data'''
    start = time.time()                        

    for file in os.listdir(path):              
        if '.csv' in file:                      
            print(file, flush=True)
            df = pd.read_csv(f'{path}/{file}')
            logging.info(f'Ingesting {file} in DB')
            ingest_db(df, file[:-4], engine)

    end = time.time()                         
    total_time = (end - start) / 60           
    logging.info('Ingestion complete')
    logging.info(f'Total time taken {total_time} minutes')

if __name__ == '__main__':
    Load_raw_data()