import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)


# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Swapnil Bhong\.jdks\corretto-1.8.0_452'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################
#from pyspark.sql import SparkSession, Window

data = [
    (101, "2025-01-01", 8, "P001"),
    (102, "2025-01-01", 6, "P002"),
    (101, "2025-01-02", 7, "P005"),
    (103, "2025-01-01", 5, "P003"),
    (102, "2025-01-02", 8, "P002"),
    (101, "2025-01-03", 9, "P004")
]
columns = ["employee_id", "date", "hours_worked", "project_id"]
df = spark.createDataFrame(data, columns)

windowSpec = Window.partitionBy("employee_id").orderBy(col("date").desc())

recent_projects_df = df.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") == 1) \
    .select("employee_id", "date", "project_id")

print("Most recent project for each employee:")
recent_projects_df.show()