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
    SELECT
        (unix_timestamp(dropOff_datetime) - unix_timestamp(pickup_datetime)) / 3600 AS trip_length_hours,
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID,
        DOlocationID,
        SR_Flag,
        Affiliated_base_number
    FROM fhv
    ORDER BY trip_length_hours desc
    LIMIT 10;
''').show()
# top result: 631152.5 hours
