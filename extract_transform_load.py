import pandas as pd
import findspark

findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkFiles
import os

# TODO: do small portions pivots as baseground to ML: <stationId, date> : <variables>
# TODO: check that uploading works with large scale data
# TODO: figure out ML with null values

#greg:
# TODO: check efficient pivot
# TODO: figure out ML with null values



def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell"

spark, sc = init_spark('demo')

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4, " \
                                    "com.microsoft.azure:spark-mssql-connector_2.11:1.1.0 pyspark-shell"

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                             "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
stations_path = 'http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt'
inventory_path = 'http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-inventory.txt'
sc.addFile(stations_path)
sc.addFile(inventory_path)


from pyspark.sql.types import *
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'

server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "dor0zehavi"
url = server_name + ";" + "databaseName=" + database_name + ";"
stations_table = "Stations_2"
inventory_table = "Inventory"
username = "dor0zehavi"
password = "Qwerty12!"

BATCH_SIZE = 1000  #should be around 1-5 mil
FINAL_SIZE = 100000  #should be 100,000,000 at the end
TABLE_NAME = "DATA_6"


def get_topics():
    """
    :return: string of all the countries code
    """
    stations = sc.textFile("ghcnd-stations.txt")
    countries = set()
    tmp = ""
    for station in stations.collect():
        if station[0:2] not in countries:
            countries.add(station[0:2])
    for country in list(countries):
        tmp = tmp + str(country) + ","
    return tmp[:len(tmp) - 1]

def create_empty_table():
    """
    creating an empty sql Table with the wanted schema
    """
    schema = StructType([StructField('StationId', StringType(), False),
                         StructField('latitude', IntegerType(), False),
                         StructField('longitude', IntegerType(), False),
                         StructField('elevation', IntegerType(), False),
                         StructField('Date', DateType(), False),
                         StructField('PRCP', IntegerType(), True),
                         StructField('SNOW', IntegerType(), True),
                         StructField('SNWD', IntegerType(), True),
                         StructField('TMAX', IntegerType(), True),
                         StructField('TMIN', IntegerType(), True)])
    empty_df = spark.createDataFrame([], schema)
    empty_df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", TABLE_NAME) \
        .option("user", username) \
        .option("password", password) \
        .save()

def create_stations_table():
    """
    creating the stations table in the Azure sql server
    :return:
    """
    stations = spark.read.text(SparkFiles.get('ghcnd-stations.txt'))
    """station_schema = StructType([StructField('StationId', StringType(), False),
                                 StructField('latitude', FloatType(), False),
                                 StructField('longitude', FloatType(), False),
                                 StructField('elevation', FloatType(), False)])"""
    stations = stations.select(stations.value.substr(0, 11).alias('StationId'),
                               stations.value.substr(13, 8).alias('latitude'),
                               stations.value.substr(22, 8).alias('longitude'),
                               stations.value.substr(32, 6).alias('elevation')).dropDuplicates(['StationId'])
    for col in ['latitude', 'longitude', 'elevation']:
        stations = stations.withColumn(col, stations[col].cast("float"))


    print()
    """stations.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", stations_table) \
        .option("user", username) \
        .option("password", password) \
        .save()"""
    return stations

def upload_batch(batch_df, batch_id):
    print("processing batch:", batch_id)
    stations = create_stations_table()
    batch_df = process_df(batch_df)
    df = stations.join(batch_df, ["StationId"], "inner")
    print()
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", TABLE_NAME) \
        .option("user", username) \
        .option("password", password) \
        .save()


def process_df(df):
    fields = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]
    fields_dfs_dict = {}
    for field in fields:
        fields_dfs_dict[field] = df.select("*")\
            .where(df["Variable"] == field)\
            .drop("Variable")\
            .withColumn(field, df["Value"]).drop("Value")
    for i in range(1,5):
        fields_dfs_dict[fields[0]] = fields_dfs_dict[fields[0]]\
            .join(fields_dfs_dict[fields[i]], ["StationId", "Date"], "left")
    return fields_dfs_dict[fields[0]]

if __name__ == '__main__':
    print()
    # Define the schema of the data:
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', IntegerType(), False),
                              StructField('Variable', StringType(), False),
                              StructField('Value', IntegerType(), False),
                              StructField('M_Flag', StringType(), True),
                              StructField('Q_Flag', StringType(), True),
                              StructField('S_Flag', StringType(), True),
                              StructField('ObsTime', StringType(), True)])
    create_empty_table()

    kafka_raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "US") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", BATCH_SIZE)\
        .load()
    kafka_value_df = kafka_raw_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    # Flatten the nested object:
    kafka_df = json_df.select("json.*")

    # ***the documentation below was here***
    query = kafka_df.limit(FINAL_SIZE) \
        .filter(F.isnull("Q_Flag") & ~F.isnull("M_flag") & ~F.isnull("S_Flag")) \
        .filter((kafka_df['Variable'] == 'PRCP') |
                (kafka_df['Variable'] == 'SNOW') | (kafka_df['Variable'] == 'SNWD') |
                (kafka_df['Variable'] == 'TMAX') | (kafka_df['Variable'] == 'TMIN')) \
        .withColumn("Date", F.to_date(F.col("Date").cast("string"), "yyyyMMdd")) \
        .select("StationId", "Date", "Variable", "Value")\
        .writeStream \
        .foreachBatch(upload_batch).start()
    query.awaitTermination()





"""vars_df = kafka_df.filter((kafka_df['Variable'] == 'PRCP') |
                              (kafka_df['Variable'] == 'SNOW') | (kafka_df['Variable'] == 'SNWD') |
                              (kafka_df['Variable'] == 'TMAX') | (kafka_df['Variable'] == 'TMIN') |
                              (kafka_df['Variable'] == 'ACMC') | (kafka_df['Variable'] == 'AWND') |
                              (kafka_df['Variable'] == 'DAPR') | (kafka_df['Variable'] == 'MDSF') |
                              (kafka_df['Variable'] == 'DATN') | (kafka_df['Variable'] == 'DATX') |
                              (kafka_df['Variable'] == 'DWPR') | (kafka_df['Variable'] == 'MDPR') |
                              (kafka_df['Variable'] == 'MDSF') | (kafka_df['Variable'] == 'EVAP'))
    vars_flags_df = vars_df.filter("Q_flag is null").filter('M_Flag is not null').filter('S_Flag is not null').drop("Q_Flag").drop("M_Flag").drop("S_Flag")
    #vars_flags_df = vars_flags_df.filter("ObsTime is not null")
    vars_flags_df.registerTempTable("test_table")
    #spark.sql("SELECT sum(CASE WHEN ObsTime is not null THEN 1 ELSE 0 END) / COUNT(*) AS ratio FROM test_table").show()
    stations = create_stations_table()
    print()
    df = stations.join(vars_flags_df.limit(1000), ["StationId"], "inner")

    pivoted_df = df.groupBy("StationId", "Date").pivot(list(set(df.schema.names)-set(["StationId", "Date"])))


    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", "DATA_2") \
        .option("user", username) \
        .option("password", password) \
        .save()
    print()
    """