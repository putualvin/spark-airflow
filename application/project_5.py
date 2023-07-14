import os
os.environ['SPARK_HOME'] = '/opt/spark'
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import desc, round
import sys

# Creating spark session
spark = SparkSession.builder\
    .master('local[*]')\
    .appName("project5")\
    .getOrCreate()
    
#Read the data
df = spark.read.parquet('/root/airflow/application/fhv_tripdata_2021-02.parquet')
df.show(5)
#Checking columns
df.printSchema()
#Creating temporary view
df.createOrReplaceTempView('sqlquery')

# 1. How many taxi trips were there on February 15?
df2 = spark.sql('''
                    select
                        count (distinct dispatching_base_num)
                    from sqlquery
                    where date_trunc('day', pickup_datetime)='2021-02-15';
                ''')
print(df2.show())

# 2. Find the longest trip for each day ?
df3 = spark.sql('''
                    select
                        date_trunc('day', pickup_datetime) as current_day,
                        max(dropOff_datetime-pickup_datetime) as duration
                    from sqlquery
                    group by 1;
                ''')
print(df3.show())

# 3. Find Top 5 Most frequent `dispatching_base_num` ?
df4 = spark.sql('''
                    select
                        dispatching_base_num,
                        count(distinct pickup_datetime) total_vechile
                    from sqlquery
                    group by 1
                    order by 2 desc
                    limit 5;
                ''')
df4.show()

# Find Top 5 Most common location pairs (PUlocationID and DOlocationID)
# if just looking at the questions
df5 = spark.sql('''
                    select
                        case 
                            when PUlocationID is null or DOlocationID is null then null
                            else concat(PUlocationID," - ",DOlocationID) end
                            as pair_id,
                        count(distinct dispatching_base_num) as total
                    from sqlquery
                    group by 1
                    order by 2 desc
                    limit 5;
                ''')
print(df5.show())
# It felt weird that the data does not have a pickup and dropoff location. And also where the pickup and the dropoff location is the same. This is another version of query
df6 = spark.sql('''
                    select
                        case 
                            when PUlocationID is null or DOlocationID is null then null
                            else concat(PUlocationID," - ",DOlocationID) end as pair_id,
                        count(distinct dispatching_base_num) as total
                    from sqlquery
                    where PUlocationID != DOlocationID
                    group by 1
                    order by 2 desc
                    limit 5;
                ''')
print(df6.show())
