import numpy as np
import pandas as pd
import pyodbc
import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkFiles
import os
from pyspark.sql.types import *


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


def load(table_name):
    server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
    database_name = "dor0zehavi"
    url = server_name + ";" + "databaseName=" + database_name + ";"
    username = "dor0zehavi"
    password = "Qwerty12!"
    data = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()
    return data


def dist_metric(station1, station2):
    """
    :param station1: station with index access
    :param station2: station with column access
    """
    sum = 0
    for i, column in enumerate(["latitude", "longitude", "elevation"]):
        diff = station1[i] - station2[column]
        sum += diff**2
    return np.sqrt(sum)


def create_dist_func(station):
    return lambda row: dist_metric(row, station)


class Predictor:
    def __init__(self, k, variable):
        self.k = k
        self.variable = variable
        self.data = load("DATA")
        self.stations = load("Stations")
        for column in ["latitude", "longitude", "elevation"]:
            self.stations = self.stations.withColumn(column, self.stations[column].cast("float"))
        self.columns = self.stations.schema.names

    def predict(self, station):
        dist_udf = F.udf(create_dist_func(station))
        columns = self.columns.copy()
        columns.remove("StationId")
        stations_dist_df = self.stations.withColumn("dist", dist_udf(F.array(self.columns)))
        print()



if __name__ == '__main__':

    stations = load("Stations")
    for column in ["latitude", "longitude", "elevation"]:
        stations = stations.withColumn(column, stations[column].cast("float"))
    stations = stations.take(5)
    predictor = Predictor(5, "PRCP")
    for station in stations:
        predictor.predict(station)
