import pyspark
from pyspark.sql import SparkSession, types

#Run PySpark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


fhv_schema = types.StructType([
    types.StructField("dispatching_base_num", types.StringType(), True ),
    types.StructField("pickup_datetime", types.TimestampType(), True ),
    types.StructField("dropOff_datetime", types.TimestampType(), True ),
    types.StructField("PUlocationID", types.IntegerType(), True ),
    types.StructField("DOlocationID", types.IntegerType(), True ),
    types.StructField("SR_Flag", types.StringType(), True ),
    types.StructField("Affiliated_base_number", types.StringType(), True )
])

df = spark.read \
    .option("header", "true") \
    .schema(fhv_schema) \
    .csv('fhv_tripdata_2019-10.csv.gz')

# write file to parquet

df \
    .repartition(6) \
    .write \
    .mode("overwrite") \
    .parquet("output_fhv_tripdata_2019-10")
