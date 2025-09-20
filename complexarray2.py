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
## COMPLEX ARRAY EXAMPLE 2

data="""{
   "name":"John",
   "age":30,
   "cars":[
      {
         "name":"Ford",
         "models":[
            "Fiesta",
            "Focus",
            "Mustang"
         ]
      },
      {
         "name":"BMW",
         "models":[
            "320",
            "X3",
            "X5"
         ]
      },
      {
         "name":"Fiat",
         "models":[
            "500",
            "Panda"
         ]
      }
   ]
}"""

df = spark.read.json(sc.parallelize([data]))

df.show()
df.printSchema()

flat1 = df.selectExpr(

    "age",
    "explode(cars) as cars",
    "name"

)

flat1.show()
flat1.printSchema()

flat2 = flat1.selectExpr(

    "age",
    "cars.models",
    "cars.name as cars_name",
    "name"

)

flat2.show()
flat2.printSchema()


finalflat = flat2.selectExpr(

    "age",
    "explode(models) as models",
    "cars_name",
    "name"

)

finalflat.show()
finalflat.printSchema()
