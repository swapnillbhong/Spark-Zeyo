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

    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None , "cash")

]
df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()


procdf = df.selectExpr(

    " cast(id as int) as id",  # Expression
    " split(tdate,'-')[2] as year ",     # Expression
    "amount +  1000  as amount",    # Expression
    " upper(category) as category   ",  # Expression
    " concat(product,'~zeyo')  as  product ",  # Expression
    "spendby",   # select
    """ case  
                   when spendby='cash'  then 0   
                   when spendby='credit'  then 1  
                   else 2   
                   end   
                   as status
               """    #add one more column

)

procdf.show()


#WITH COLUMN CODE


data = [

    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None , "cash")

]
df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()



withdf = (

    df.withColumn("category",expr("upper(category)"))
    .withColumn("amount",expr("amount+1000"))
    .withColumn("product",expr("concat(product,'~zeyo')"))
    .withColumn("id",expr("cast(id as int)"))
    .withColumn("tdate",expr("split(tdate,'-')[2]"))
    .withColumn("status",expr("case when spendby='cash' then 0 else 1 end as newstatus"))
    .withColumnRenamed("tdate","year")

)

withdf.show()

# JOINS

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()
data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]
prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()


innerjoin = cust.join(  prod , ["id"], "inner")
innerjoin.show()

lefjoin  =   cust.join(prod, ["id"], "left")
lefjoin.show()

rightjoin = cust.join(prod, ["id"], "right")
rightjoin.show()

fulljoin = cust.join(prod, ["id"],"full")
fulljoin.show()



