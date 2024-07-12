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



def main():
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"} 

    curr_dir = os.getcwd()
    print('**************')
    print(curr_dir)
    with open('opt/airflow/python_logic/credentials.yaml', 'r') as f:
        doc = yaml.load(f)

    access_key = doc['s3']['access_key']
    secret_access_key = doc['s3']['secret_access_key']
    bucket = 'housing-data-docker'
    session = boto3.Session(aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, bucket = bucket)
    s3 = session.resource('s3')
    home_dir = {}
    zipcodes = ['53151', '53110', '53220', '53209']
    for zip in zipcodes:
        url = f'https://www.redfin.com/zipcode/{zip}'
        r = requests.get(url, headers = headers)
        print(r.request.headers['User-Agent'])
        soup = BeautifulSoup(r.content, 'html5lib')
        table = soup.find('div' , attrs = {"id" : "results-display"})
        for row in table.find_all('div' , attrs = {"class" : "HomeCardContainer"}):
            try:
                
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
    
            except:
                pass

    table_data = []
    column_names = []
    for key,value in home_dir.items():
        row_data = []
        for k,v in value.items():
            if k not in column_names:
                column_names.append(k)
            row_data.append(v)
        table_data.append(row_data)

    # print(table_data)
    # print(column_names)
    df = pd.DataFrame(table_data, columns = column_names)
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # csv_buffer = StringIO()
    # df.to_csv(csv_buffer)

    # s3.Object(bucket, f'home_data/meta_load_dt={current_timestamp}/home_data.csv').put(Body=csv_buffer.getvalue())

    df.to_csv(f's3://housing-data-docker/redfin-data/meta_load_dt={current_timestamp}/home_data.csv')





    # current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    # parent_path = os.chdir('..')
    # print("***********************")
    # print(current_dir)
    # print(parent_path)
    # current_path = r"opt/airflow/{current_dir}/home_data/meta_load_dt={current_timestamp}".format(current_dir=current_dir,current_timestamp=current_timestamp)
    # if not os.path.exists(current_path):
    #     os.makedirs(current_path)
    # # df.to_csv(r'opt/airflow/home_data/meta_load_dt={current_timestamp}/home_data.csv'.format(current_timestamp=current_timestamp))
    # df.to_csv(r'opt/airflow/home_data/home_data.csv')



if __name__ == '__main__':
    main()
   