import pandas as pd
import findspark

findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkFiles
import os


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

from pyspark.sql.types import *
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'

server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "dor0zehavi"
url = server_name + ";" + "databaseName=" + database_name + ";"
stations_table = "Stations"
inventory_table = "Inventory"
username = "dor0zehavi"
password = "Qwerty12!"

def get_topics():
    """
    :return: string\list of all the countries code
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


def create_stations_table():
    """
    creating the stations table in the Azure sql server
    :return:
    """

    stations_path = 'http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt'
    inventory_path = 'http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-inventory.txt'
    sc.addFile(stations_path)
    sc.addFile(inventory_path)

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
    stations.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", stations_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    return stations

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

    kafka_raw_df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", get_topics()) \
        .option("startingOffsets", "earliest") \
        .load()
    # TODO: notice i have subscribed only to USA, its just for quick working- need to sub. to more topics (get_topics)
    kafka_value_df = kafka_raw_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    # Flatten the nested object:
    kafka_df = json_df.select("json.*")
    # TODO: maybe to choose different variables to filter by?
    vars_df = kafka_df.filter((kafka_df['Variable'] == 'PRCP') |
                              (kafka_df['Variable'] == 'SNOW') | (kafka_df['Variable'] == 'SNWD') |
                              (kafka_df['Variable'] == 'TMAX') | (kafka_df['Variable'] == 'TMIN') |
                              (kafka_df['Variable'] == 'ACMC') | (kafka_df['Variable'] == 'AWND') |
                              (kafka_df['Variable'] == 'DAPR') | (kafka_df['Variable'] == 'MDSF') |
                              (kafka_df['Variable'] == 'DATN') | (kafka_df['Variable'] == 'DATX') |
                              (kafka_df['Variable'] == 'DWPR') | (kafka_df['Variable'] == 'MDPR') |
                              (kafka_df['Variable'] == 'MDSF') | (kafka_df['Variable'] == 'EVAP'))
    vars_flags_df = vars_df.filter("Q_flag is null").filter('M_Flag is null').filter('S_Flag is not null')
    stations = create_stations_table()
    df = vars_flags_df.join(stations, vars_flags_df.StationId == stations.StationId, "inner")
    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", "DATA") \
        .option("user", username) \
        .option("password", password) \
        .save()
    print()

