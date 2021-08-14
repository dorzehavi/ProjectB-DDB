import pyodbc
import pandas as pd
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

def fragmentation_data_analysis(df):  # TODO: df sholud be the table DATA from the sql server


    # TODO separate to different file
    # temporal:
    df_PRCP = df.filter(df['Variable'] == 'PRCP')
    winter_df = df_PRCP.filter(
        (df_PRCP["Date"][4:6] == "12") | (df_PRCP["Date"][4:6] == "01") | (df_PRCP["Date"][4:6] == "02"))
    spring_df = df_PRCP.filter(
        (df_PRCP["Date"][4:6] == "03") | (df_PRCP["Date"][4:6] == "04") | (df_PRCP["Date"][4:6] == "05"))
    summer_df = df_PRCP.filter(
        (df_PRCP["Date"][4:6] == "06") | (df_PRCP["Date"][4:6] == "07") | (df_PRCP["Date"][4:6] == "08"))
    fall_df = df_PRCP.filter(
        (df_PRCP["Date"][4:6] == "09") | (df_PRCP["Date"][4:6] == "10") | (df_PRCP["Date"][4:6] == "11"))

    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=technionddscourse.database.windows.net;'
                          'DATABASE=dor0zehavi;UID=dor0zehavi;PWD=Qwerty12!')
    cursor = conn.cursor()
    SQL_Query = pd.read_sql_query(
        '''select *
        from Stations''', conn)

    stations = pd.DataFrame(SQL_Query, columns=['StationId', 'latitude', 'longitude', 'elevation'])
    # spatial:
    stations_data_df = df_PRCP.join(stations, df_PRCP.StationId == stations.StationId, 'inner')
    Q1_df = stations_data_df.filter(stations_data_df["latitude"] >= 0 & stations_data_df["longitude"] > 0)
    Q2_df = stations_data_df.filter(stations_data_df["latitude"] >= 0 & stations_data_df["longitude"] < 0)
    Q3_df = stations_data_df.filter(stations_data_df["latitude"] <= 0 & stations_data_df["longitude"] < 0)
    Q4_df = stations_data_df.filter(stations_data_df["latitude"] < 0 & stations_data_df["longitude"] >= 0)

    quarters = [Q1_df, Q2_df, Q3_df, Q4_df]
    quarters_seasons = []
    for i in range(4):
        quarter_season = list()
        # Q_i & winter
        quarter_season.append(quarters[i].filter(
            (quarters[i]["Date"][4:6] == "12") | (quarters[i]["Date"][4:6] == "01") | (quarters[i]["Date"][4:6] == "02")))
        # Q_i & spring
        quarter_season.append(quarters[i].filter(
            (quarters[i]["Date"][4:6] == "03") | (quarters[i]["Date"][4:6] == "04") | (quarters[i]["Date"][4:6] == "05")))
        # Q_i & summer
        quarter_season.append(quarters[i].filter(
            (quarters[i]["Date"][4:6] == "06") | (quarters[i]["Date"][4:6] == "07") | (quarters[i]["Date"][4:6] == "08")))
        # Q_I & fall
        quarter_season.append(quarters[i].filter(
            (quarters[i]["Date"][4:6] == "09") | (quarters[i]["Date"][4:6] == "10") | (quarters[i]["Date"][4:6] == "11")))
        quarters_seasons.append(quarter_season)

        # TODO: upload all these df to the Azure sql server- for the insight