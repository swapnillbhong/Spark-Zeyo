import os
import urllib.request
import ssl


data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

# Set HADOOP_HOME environment variable
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


# Sample JSON data as a multi-line string
data = """
{
  "first_name": "Rajeev",
  "last_name": "Sharma",
  "email_address": "rajeev@ezeelive.com",
  "is_alive": true,
  "age": 30,
  "height_cm": 185.2,
  "billing_address": {
    "address": "502, Main Market, Evershine City, Evershine, Vasai East",
    "city": "Vasai Raod, Palghar",
    "state": "Maharashtra",
    "postal_code": "401208"
  },
  "shipping_address": {
    "address": "Ezeelive Technologies, A-4, Stattion Road, Oripada, Dahisar East",
    "city": "Mumbai",
    "state": "Maharashtra",
    "postal_code": "400058"
  },
 "date_of_birth": null
}
"""

# Create an RDD from the JSON string
jsonrdd = spark.sparkContext.parallelize([data])


# Create a DataFrame from the RDD, treating it as a multi-line JSON
df = spark.read.option("multiline","true").json(jsonrdd)


# Show the original DataFrame and its schema
df.show()
df.printSchema()

# Flatten the DataFrame by selecting and renaming nested fields
flatdf = df.selectExpr(
    "age",
    "billing_address.address   as   billing_address",
    "billing_address.city   as   billing_city",
    "billing_address.postal_code as billing_postal_code",
    "billing_address.state as billing_state",
    "date_of_birth",
    "email_address",
    "first_name",
    "height_cm",
    "is_alive",
    "last_name",
    "shipping_address.address as shipping_address",
    "shipping_address.city as shipping_city",
    "shipping_address.postal_code as shipping_postal",
    "shipping_address.state as shipping_state"
)

# Show the flattened DataFrame and its schema
flatdf.show()
flatdf.printSchema()
