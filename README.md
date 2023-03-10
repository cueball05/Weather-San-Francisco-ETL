# Weather-San-Francisco-ETL

## Introduction
This is a ETL pipeline script that uses Docker for deployment and Apache Airflow for management and scheduling. The workflow first makes a GET request to a web API hosted by National Oceanic and Atmospheric Administration (NOAA). Data retreived from the web API will be placed into a dataframe and loaded to Postgresql. 

### Docker Containers
The yaml file `airflow-init.yaml` contains the required services to run the ETL script. Apache Airflow and Postgresql are defined in the yaml file. Apache Airflow will manage the DAGs and Postgresql will be the database to hold the data retreived from the web API

The Docker containers can be initialized by using the command:

`docker-compose -f airflow-init.yaml up`

And similarly to deactivate the containers, use command:

`docker-compose -f airflow-init.yaml down`

### Airflow DAGs
The .py file `airflow_weather_dag.py` in the `dags` folder contains the relevant information required to initialize a DAG. 

```
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
```

The code above defines a DAG with the name `weather-sf-dag` holding the default arguments defined above. It is scheduled to run every day (`schedule_interval=dt.timedelta(days=1)`) although other time intervals are also [possible](https://medium.com/apply-data-science/airflow-tutorial-4-writing-your-first-pipeline-6ebcd0b7bbeb). 

```
with dag:    

    run_etl = PythonOperator(
        task_id='weather_etl_final',
        python_callable=ETL,
        dag=dag,
    )

    run_etl
```

The above code defines a tasks called `run_etl`. It uses a `PythonOperator` to execute the `ETL` function defined earlier in `airflow_weather_dag.py`. At the end of the code, the task `run_etl` is executed. As there can be multiple tasks in a DAG, it is important to define the tasks dependencies either through bitshift operators `>>` or `<<`, or more explicitly, `set_upstream` or `set_upstream` methods. However, as there is only one task defined in this code, no task dependencies are set. 

### ETL Pipeline Script
Using the web API requires a token which can be requested [here](https://www.ncdc.noaa.gov/cdo-web/token). Next, a weather station needs to be chosen to collect the data from which can be accessed [here](https://www.ncdc.noaa.gov/cdo-web/datatools/findstation). In this case, the station USW00023234 of San Franciso International Airport, CA US was chosen.

The base request is as below:

`https://www.ncei.noaa.gov/access/services/data/v1?` 

After which additional parameters are appended. For a list of supported parameter keys and values see [here](https://github.com/partytax/ncei-api-guide).


