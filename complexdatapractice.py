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

finaldf = ((df.withColumn("City",expr("address.city as city"))
           .withColumn("street",expr("address.street as street"))
           .withColumn("zip",expr("address.zip as zip"))
           .withColumn("contacts",expr("explode(contacts)")))
           .withColumn("Contact_type",expr("contacts.type as Contact_type"))
           .withColumn("contact_value",expr("contacts.value as contact_value"))
           .drop("address","contacts"))
finaldf.show()
finaldf.printSchema()

