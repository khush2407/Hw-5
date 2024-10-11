from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import requests

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='stock_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 10, 9), 
    catchup=False,
    schedule_interval='@daily',
    tags=['stock', 'MSFT'],
) as dag:

    @task
    def extract(symbol):
        return symbol

    @task
    def return_last_90d_price(symbol):
        vantage_api_key = Variable.get('vantage_api_key')
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
        r = requests.get(url)
        data = r.json()
        results = []
        if "Time Series (Daily)" in data:
            for d in data["Time Series (Daily)"]:
                stock_info = data["Time Series (Daily)"][d]
                stock_info['date'] = d
                results.append(stock_info)
        return results

    @task
    def create_table():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        create_table_query = """
            CREATE OR REPLACE TABLE raw_data.stock_price (
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                date DATE PRIMARY KEY
            )
        """

        with conn.cursor() as cur:
            try:
                cur.execute("USE DATABASE ASSIGNMENT_4_DATA226")  
                cur.execute("USE SCHEMA RAW_DATA") 
                cur.execute(create_table_query)
            except Exception as e:
                print(f"Error occurred while creating table: {e}")
                raise

        conn.close()

    @task
    def load_records(data):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        insert_sql = """
            INSERT INTO raw_data.stock_price (open, high, low, close, volume, date)
            VALUES (%(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(date)s)
        """

        with conn.cursor() as cur:
            try:
                cur.execute("USE DATABASE ASSIGNMENT_4_DATA226")  
                cur.execute("USE SCHEMA RAW_DATA")  

                for r in data:
                    records = {
                        'open': r['1. open'],
                        'high': r['2. high'],
                        'low': r['3. low'],
                        'close': r['4. close'],
                        'volume': r['5. volume'],
                        'date': r['date']
                    }
                    cur.execute(insert_sql, records)
            except Exception as e:
                print(f"Error occurred while inserting records: {e}")
                raise

        conn.close()

    # Define the pipeline
    symbol = 'MSFT'
    raw_symbol = extract(symbol)
   # extract(symbol=symbol) >> return_last_90d_price(raw_symbol) 
    stock_data = return_last_90d_price(raw_symbol)
    create_table()
    load_records(stock_data)