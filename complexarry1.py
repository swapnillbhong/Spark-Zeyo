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

## COMPLEX ARRAY EXAMPLE 1

data="""{
  "country" : "US",
  "version" : "0.6",
  "Actors": [
    {
      "name": "Tom Cruise",
      "age": 56,
      "BornAt": "Syracuse, NY",
      "Birthdate": "July 3, 1962",
      "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
      "wife": null,
      "weight": 67.5,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/73.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
                }
    },
    {
      "name": "Robert Downey Jr.",
      "age": 53,
      "BornAt": "New York City, NY",
      "Birthdate": "April 4, 1965",
      "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
      "wife": "Susan Downey",
      "weight": 77.1,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/78.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
                }
    }
  ]
}"""

df = spark.read.json(sc.parallelize([data]))


df.show()
df.printSchema()

flat1 = df.selectExpr(

    "explode(Actors) as Actors",
    "country",
    "version"


)

flat1.show()
flat1.printSchema()

flat2 = flat1.selectExpr(

    "Actors.Birthdate",
    "Actors.BornAt",
    "Actors.age",
    "Actors.hasChildren",
    "Actors.hasGreyHair",
    "Actors.name",
    "Actors.photo",
    "Actors.picture.large",
    "Actors.picture.medium",
    "Actors.picture.thumbnail",
    "Actors.weight",
    "Actors.wife",
    "country",
    "version"


)

flat2.show()
flat2.printSchema()