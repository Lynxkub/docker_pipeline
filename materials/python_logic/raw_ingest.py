from bs4 import BeautifulSoup
import requests
import html5lib
import pandas as pd
import datetime
import os
import sys
import boto3
import yaml
from io import StringIO
from airflow.models import Variable
import s3fs
import numpy as np


def main():
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"} 

    access_key = Variable.get('aws_access_key')
    secret_access_key = Variable.get('aws_secret_access_key')



    bucket = 'housing-data-docker'
    session = boto3.Session(aws_access_key_id = access_key, aws_secret_access_key = secret_access_key)
    s3 = session.resource('s3')
    home_dir = {}
    zipcodes = Variable.get('zipcodes')
    print(zipcodes)
    for zip in zipcodes:
        print(f'finding housing info for {zip}')
        url = f'https://www.redfin.com/zipcode/{zip}'
        r = requests.get(url, headers = headers)
        print(r.request.headers['User-Agent'])
        soup = BeautifulSoup(r.content, 'html5lib')
        table = soup.find('div' , attrs = {"id" : "results-display"})
        # try:
        for row in table.find_all('div' , attrs = {"class" : "HomeCardContainer"}):
            
            price = row.find('span', attrs = {'class' : 'bp-Homecard__Price--value'}).text
            address = row.find('div', attrs = {"class" : 'bp-Homecard__Address flex align-center color-text-primary font-body-xsmall-compact'}).text
            house_url = row.find('a', attrs = {'class' : 'link-and-anchor visuallyHidden'})['href']
            city = house_url.split('/')[2].replace('-', ' ')
            # print(city)
            beds = row.find('span' , attrs = {'class' : 'bp-Homecard__Stats--beds text-nowrap'}).text
            if beds == '— beds':
                beds = 'No Data Available'
            baths = row.find('span', attrs = {'class' , 'bp-Homecard__Stats--baths text-nowrap'}).text
            if baths == '— baths':
                baths = 'No Data Available'
            sqft = row.find('span', attrs = {'class' , 'bp-Homecard__LockedStat--value'}).text
            if sqft == '— ':
                sqft = 'No Data Available'

            home_dir[address] = {'city' : city,
                                'price' : price,
                                'beds' : beds,
                                'baths' : baths,
                                'sqft' : sqft,
                                'url' : 'https://www.redfin.com' + house_url,
                                'address': address}
    
        # except:
        #     pass

    table_data = []
    column_names = []
    print(home_dir)
    for key,value in home_dir.items():
        row_data = []
        for k,v in value.items():
            if k not in column_names:
                column_names.append(k)
            row_data.append(v)
        table_data.append(row_data)


    df = pd.DataFrame(table_data, columns = column_names)
    
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    Variable.set("meta_load_dt", current_timestamp)



    # df['ins_dttm'] = current_timestamp
    # df['extract_dttm'] = current_timestamp
    # df['delete_dttm'] = np.nan
    # df['delete_dttm'] = pd.to_datetime(df['delete_dttm'], errors = 'coerce')
    print(df.head())
    df.to_csv(f's3://housing-data-docker/redfin-data/raw_data/meta_load_dt={current_timestamp}/home_data.csv', storage_options = {
        "key": access_key,
        "secret": secret_access_key
    })




if __name__ == '__main__':
    main()
   