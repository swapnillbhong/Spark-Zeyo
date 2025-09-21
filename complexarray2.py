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
## WITHCOLUMN

data="""{
  "id": 1,
  "name": "Sai",
  "address": {
    "street": "123 Main St",
    "city": "Hyderabad",
    "zip": "500081"
  },
  "contacts": [
    {
      "type": "email",
      "value": "sai@example.com"
    },
    {
      "type": "phone",
      "value": "‪+91-9876543210‬"
    }
  ]
}"""

df = spark.read.json(sc.parallelize([data]))

df.show()
df.printSchema()

arrayflat = df.withColumn("contacts",expr("explode(contacts)"))

arrayflat.show()
arrayflat.printSchema()

finalflat = arrayflat.select(

    "address.city",
    "address.street",
    "address.zip",
    "contacts.type",
    "contacts.value",
    "id",
    "name"
)
finalflat.show()
finalflat.printSchema()


# URL DATA

import urllib
import ssl

urldata = urllib.request.urlopen("https://randomuser.me/api/0.8/?results=10",context=ssl._create_unverified_context()).read().decode('utf-8')


print(urldata)


df   = spark.read.json(sc.parallelize([urldata]))

df.show()
df.printSchema()



explodedf = df.withColumn("results" , expr("explode(results)"))

explodedf.show()
explodedf.printSchema()


finalflatten = explodedf.select(

    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"
)

finalflatten.show()
finalflatten.printSchema()