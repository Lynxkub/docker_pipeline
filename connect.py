import psycopg2
import pandas as pd
from config import load_config
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def connect(config):
    # try:
    #     with psycopg2.connect(**config) as conn:
    #         print('Connected to the PostgreSQL server.')
    #         return conn
    # except (psycopg2.DatabaseError, Exception) as error:
    #     print(error)
    try:
        url = URL.create(
            drivername = 'postgresql',
            username='postgres',
            host = 'localhost',
            database='realestate'
        )
        engine = create_engine('postgresql+psycopg2://postgres:admin@localhost/realestate')
        connection = engine.connect()
        # cursor = connection.cursor()
        return connection
    except:
        print('connection not made')

def write_to_table(conn, file):
    df = pd.read_csv(file)
    print('read in file correctly')
    df.to_sql('house_data', conn)
    return df


if __name__ =='__main__':
    config = load_config()
    conn = connect(config)
    write_to_table(conn, 'home_data.csv')
    
