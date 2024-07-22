import boto3
import pandas as pd
import datetime
from airflow.models import Variable

def main():
    meta_load_dt = Variable.get('meta_load_dt')
    access_key = Variable.get('aws_access_key')
    secret_access_key = Variable.get('aws_secret_access_key')
    first_load_flag = Variable.get('first_load_flag')


    if first_load_flag == 'True':
        try:
            df_raw = pd.read_parquet(f's3://housing-data-docker/redfin-data/raw_data/meta_load_dt={meta_load_dt}/home_data.parquet',  storage_options = {
            "key": access_key,
            "secret": secret_access_key
            })
        
        except FileNotFoundError as e:
            print(e)
        
        df_raw['active_flag'] = '1'
        df_raw['delete_flag'] = '0'
        df_raw['meta_load_dt'] = meta_load_dt


        df_raw.to_csv(f's3://housing-data-docker/redfin-data/curated/meta_load_dt={meta_load_dt}/curated_home_data.parquet', storage_options = {
        "key": access_key,
        "secret": secret_access_key
    })
        Variable.set('first_load_flag' , 'False')
    

    else:
        prev_meta_load_dt = Variable.get('prev_meta_load_dt')
        try:
            df_raw = pd.read_parquet(f's3://housing-data-docker/redfin-data/raw_data/meta_load_dt={meta_load_dt}/home_data.parquet',  storage_options = {
            "key": access_key,
            "secret": secret_access_key
            })
            
        except FileNotFoundError as e:
            print(e)

        try:
            df_curated_previous = pd.read_parquet(f's3://housing-data-docker/redfin-data/curated/meta_load_dt={prev_meta_load_dt}/home_data.parquet', storage_options = {
                "key": access_key,
                "secret" : secret_access_key
            })
        except FileNotFoundError as e:
            print(e)

        new_records_df = ''
        deletes_df = ''
        updates_df = ''
        unchanged_df = ''




    Variable.set('prev_meta_load_dt', meta_load_dt)
if __name__ == "__main__":
    main()