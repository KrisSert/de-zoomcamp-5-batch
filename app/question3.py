import pyspark
from pyspark.sql import SparkSession, types

#Run PySpark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet('output_fhv_tripdata_2019-10/*')
df.registerTempTable('fhv')

# run query to find trips w start date: 
spark.sql('''
    SELECT count(*)
    FROM fhv
    WHERE TO_DATE(pickup_datetime) = '2019-10-15';
''').show()
# result: 62610
