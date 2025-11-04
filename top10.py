import os
import urllib.request
import ssl

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)


# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Swapnil Bhong\.jdks\corretto-1.8.0_452'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'git
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#swapnil


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

data = [
    ("2025-11-04", "P001", 5),
    ("2025-11-04", "P002", 3),
    ("2025-11-04", "P001", 2),
    ("2025-11-04", "P003", 8),
    ("2025-11-04", "P002", 4),
    ("2025-11-04", "P004", 10),
    ("2025-11-03", "P001", 7)
]
schema = StructType([
    StructField("date", StringType(), True), # Initially read as string
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)

df.show()


day_df = df.filter(col("date")=="2025-11-04")
day_df.show()

agg_df = day_df.groupBy("product_id").agg(sum("quantity").alias("total_sold"))
agg_df.show()

top_10 = agg_df.orderBy(col("total_sold").desc()).limit(10)
top_10.show()

