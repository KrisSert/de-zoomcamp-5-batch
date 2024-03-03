import pyspark
from pyspark.sql import SparkSession

#Run PySpark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.show()


df.write.parquet('zones')

# Print Spark version
print("Spark version:", spark.version)

