import findspark
findspark.init()
from pyspark.sql import SparkSession
import os
import time
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


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


start_load = time.time()
data_df = load("BIG_DATA_4")
data_df = data_df.drop("StationId")
input_cols = data_df.columns
input_cols.remove("PRCP")
data_df = data_df.na.drop()
vector_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
df_temp = vector_assembler.transform(data_df)
data_df = df_temp.drop(*input_cols)
train_df, test_df = data_df.randomSplit([0.7, 0.3])
print("Count:", data_df.count())
finish_load = time.time()
print("Load time:", finish_load - start_load)
for num_trees in [15]:  # [5, 10, 15, 20, 25, 30]:
    start_iter = time.time()
    rf = RandomForestRegressor(featuresCol="features",
                               labelCol="PRCP",
                               numTrees=num_trees)
    model = rf.fit(train_df)
    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="PRCP",
                                    predictionCol="prediction",
                                    metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    finish_iter = time.time()
    print("Num Trees:", num_trees)
    print("RMSE:", rmse)
    print("Iteration time:", finish_iter - start_iter)


if __name__ == '__main__':
    pass