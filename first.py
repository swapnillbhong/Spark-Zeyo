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
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################


data = [

    ("DEPT1", 1000),
    ("DEPT1", 500),
    ("DEPT1", 700),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 200),
    ("DEPT3", 500)
]

columns = ["department", "salary"]

df = spark.createDataFrame(data, columns)

df.show()


from pyspark.sql.window import Window
from pyspark.sql.functions import *


createwindow = Window.partitionBy("department").orderBy(col("salary").desc())

denserankdf = df.withColumn("denserank",dense_rank().over(createwindow))
denserankdf.show()

rankdf = df.withColumn("rank",rank().over(createwindow))
rankdf.show()

print("======rank seen")
rankdf2 = rankdf.filter("rank = 2")
rankdf2.show()

findf = denserankdf.filter("denserank = 2")
findf.show()

print("========= dense ranking =======")
finaldf = findf.drop("denserank")
finaldf.show()

print("======= ranking======")
rankdf2 = rankdf2.drop("rank")
rankdf2.show()
