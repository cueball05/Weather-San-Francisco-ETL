#!/usr/bin/env python
# coding: utf-8

# In[6]:
from datetime import datetime, date
from datetime import timedelta
import psycopg2
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago
from weather_etl_delta import weather_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023,2,8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'weather-sf-dag',
    default_args=default_args,
    description='SF weather data ETL',
    schedule_interval=dt.timedelta(days=1),
)


def ETL():
    print("started")
    conn = BaseHook.get_connection('postgre_sql')
    #create db engine
    db = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    #connect to engine
    db_conn = db.connect()
    #connect to postgresql db 
    connection = psycopg2.connect(user=conn.login,
                              password=conn.password,
                              host=conn.host,
                              port=conn.port,
                              database=conn.schema)
    
    connection.autocommit = True
    cursor = connection.cursor()
    #define sql statement
    sql1 = 'select "DATE" from weather_data.sf_data order by "DATE" desc limit 1;'
    #execute sql statement
    cursor.execute(sql1)
    #fetch result
    result = cursor.fetchone()
    
    date_offset = result[0] + timedelta(days=1)
    startDate = date_offset.strftime('%Y-%m-%d')
    
    #endDate is equivalent to getting yesterdays date
    yesterday_date = date.today() - timedelta(days=1)
    endDate = yesterday_date.strftime('%Y-%m-%d')
    
    
    df=weather_etl(startDate,endDate)
    #db = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    #conn = db.connect()
    df.to_sql('sf_data', schema = 'weather_data',con = db, if_exists='append', index=False)
    #conn = psycopg2.connect(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    connection.commit()
    connection.close()

with dag:    

    run_etl = PythonOperator(
        task_id='weather_etl_final',
        python_callable=ETL,
        dag=dag,
    )

    run_etl
    

