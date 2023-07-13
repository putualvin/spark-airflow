import os
os.environ['SPARK_HOME'] = '/opt/spark'
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import desc, round
import sys

#Defining spark
spark = SparkSession.builder\
    .master('local[*]')\
    .appName("RetailData")\
    .getOrCreate()
#Defining schema
schema = StructType([\
    StructField('InvoiceNo', StringType(), True), \
    StructField('StockCode', StringType(), True), \
    StructField('Description', StringType(), True), \
    StructField('Quantity', IntegerType(), True),\
    StructField('InvoiceData', TimestampType(), True),\
    StructField('Amount', FloatType(), True),\
    StructField('CustomerID', FloatType(), True), \
    StructField('Country', StringType(), True)])

#Reading CSV
df = spark.read.csv('application/retail-data-full.csv',
                    header=False,
                    schema=schema,
                    sep = ';')
result = df.select('CustomerID', round('amount',2).alias('rounded_amount')).groupBy('CustomerID').min('rounded_amount')
result.show(5)
sorted_result = result.orderBy(desc('min(rounded_amount)'))
print(sorted_result.show())
spark.stop()