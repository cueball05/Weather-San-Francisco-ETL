# Weather-San-Francisco-ETL

## Introduction
This is a ETL pipeline script that uses Docker for deployment and Apache Airflow for management and scheduling. The workflow first makes a GET request to a web API hosted by National Oceanic and Atmospheric Administration (NOAA). Data retreived from the web API will be placed into a dataframe and loaded to Postgresql. 

### Docker Containers
The yaml file `airflow-init.yaml` contains the required services to run the ETL script. Apache Airflow and Postgresql are defined in the yaml file. Apache Airflow will manage the DAGs and Postgresql will be the database to hold the data retreived from the web API

The Docker containers can be initialized by using the command:

`docker-compose -f airflow-init.yaml up`

And similarly to deactivate the containers, use command:

`docker-compose -f airflow-init.yaml down`

###Airflow DAG
