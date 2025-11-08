import os
import urllib.request
import ssl

from pyspark.sql.types import Row

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
hadoop_home = os.path.abspath("hadoop")
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Swapnil Bhong\.jdks\corretto-1.8.0_452'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

#os.environ['PATH'] += ";" + os.path.join(hadoop_home, "bin")
#os.system(f'"{os.path.join(hadoop_home, "bin", "winutils.exe")}" chmod 777 /tmp')

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///D:/spark-streaming-sql-kinesis-connector_2.12-1.2.1.jar pyspark-shell'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################
dataA = [
    Row(ID=0),
    Row(ID=1),
    Row(ID=1),
    Row(ID=None)
]

dfA = spark.createDataFrame(dataA)
dataB = [
    Row(ID=0),
    Row(ID=0),
    Row(ID=1),
    Row(ID=1),
    Row(ID=0),
    Row(ID=None)
]

dfB = spark.createDataFrame(dataB)

inner_join = dfA.join(dfB ,"id","inner")
inner_join.show()

count_join = inner_join.count()
print(count_join)

countsql = spark.sql("select count(*) from dfA inner join b on a.id= b.id")
print(countsql)