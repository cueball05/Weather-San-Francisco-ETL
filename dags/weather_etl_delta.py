#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime, date
from datetime import timedelta
import datetime
import psycopg2
#import pandas as pd
from sqlalchemy import create_engine

NOAA_token = "VWuUrnFQHWmOODezsZGYPRDclLxfFSQr"

print('started')

def date_delta():
    #get the date delta from "DATE" column in sf_data table
    connection = psycopg2.connect(user="airflow",
                              password="airflow",
                              host="postgres",
                              port="5432",
                              database="airflow")
    #connection = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
    #db = create_engine(connection)
    cursor = connection.cursor()
    #conn = db.connect()
   
    #cursor = conn.cursor()
  
    sql1 = 'select "DATE" from weather_data.sf_data order by "DATE" desc limit 1;'
    cursor.execute(sql1)
    result = cursor.fetchone()
    #result_format = result[0].strftime('%Y-%m-%d') 
    
    start_Date = result[0] + timedelta(days=1)
    
    #start_Date = date.today() + timedelta(days=1)
    #start_Date.strftime('%Y-%m-%d')
    
    start_Date = start_Date.strftime('%Y-%m-%d')
    connection.close()
    return start_Date


def return_dataframe(startDate, endDate):
    #initialise empty dataframe to take in data
    cols = ['DATE','TMAX','TAVG','TMIN','PRCP','PGTM']
    df_weather = pd.DataFrame(columns=cols)
        
    #make the api call
    r = requests.get('https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&dataTypes=PGTM,PRCP,TAVG,TMAX,TMIN&limit=1000&stations=USW00023234&format=json&startDate='+startDate+'&endDate='+endDate+'&units=metric', headers={'Token':NOAA_token})
    #load the api response as a json
    d = json.loads(r.text)
    df_temp = pd.DataFrame.from_dict(d)
    df_temp = df_temp.drop('STATION', axis =1)
    df_weather = df_weather.append(df_temp)
    
    return df_weather
    
def format_dataframe(load_df):
    #Select all columns and strip the whitespace
    df_obj = load_df.select_dtypes(['object'])
    load_df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    #format DATE column to date format
    load_df['DATE'] = pd.to_datetime(load_df['DATE'], format='%Y-%m-%d')
    #format columns to floats
    load_df = load_df.astype({'TMAX':'float','TAVG':'float','TMIN':'float','PRCP':'float'})
    #format to time
    load_df['PGTM'] = pd.to_datetime(load_df['PGTM'], format='%H%M').dt.time
    return load_df
    
def weather_etl(startDate, endDate):
    #get weather
    load_df = return_dataframe(startDate, endDate)
    
    #calling the formatting
    formatted_df=format_dataframe(load_df)
    print(formatted_df)
    return(formatted_df)

'''def ingest_data(formatted_df):
    #put data to Postgres
    conn_string = 'postgresql://airflow:airflow@localhost:5432/airflow'

    db = create_engine(conn_string)
    conn = db.connect()

    # Create DataFrame
    formatted_df.to_sql('sf_data', schema = 'weather_data', con=conn, if_exists='append',
              index=False)
    conn = psycopg2.connect(conn_string
                            )
    conn.autocommit = True
    cursor = conn.cursor()

    conn.close()'''

    


# In[5]:


#startDate = date_delta()
#endDate is equivalent to getting yesterdays date
#yesterday_date = date.today() - timedelta(days=1)
#endDate = yesterday_date.strftime('%Y-%m-%d')
#weather_etl(startDate, endDate)
#ingest_data(formatted_df)


# In[45]:


#endDate
    #start_Date = date.today() + timedelta(days=1)
    #start_Date #.strftime('%Y-%m-%d')

