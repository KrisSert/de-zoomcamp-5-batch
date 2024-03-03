import pyspark
from pyspark.sql import SparkSession, types

#Run PySpark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet('output_fhv_tripdata_2019-10/*')
df.registerTempTable('fhv')

df_lookup = spark.read.option("header", "true").csv('taxi+_zone_lookup.csv')
df_lookup.registerTempTable('zones')

# run query to find least frequent zones: 
spark.sql('''
    select
          count(fhv.PUlocationID) as trips_amount, 
          zones.Zone
    FROM fhv
    LEFT JOIN zones
    ON (fhv.PUlocationID = zones.LocationID)
    WHERE zones.LocationID is not null
    GROUP BY
        fhv.PUlocationID,
        zones.Zone
    ORDER BY
        trips_amount asc
    LIMIT 5;
''').show()
