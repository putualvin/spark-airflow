# Running Spark on Airflow
The project consits of two folders, application and dags. The application folder contains the spark script needed to run and the dags will contain 
the script to run spark on airflow. 

## application folder
There are file and scripts used for this

- ``` fhv_tripdata_2021-02.parquet``` is the file use for spark
- ``` project_5.py``` contains the answer and also the script used to run spark

## dags Folder
Folder used to store dag scripts and to run airflow

- ```project-5_spark.py``` to execute and run it at airflow
