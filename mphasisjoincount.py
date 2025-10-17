
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

# Define the schema for the tables
schema = StructType([
    StructField("ID", IntegerType(), True)
])

# Create data for Table A and Table B
data_a = [(0,), (1,), (1,), (None,)]
data_b = [(0,), (0,), (1,), (1,), (0,), (None,)]

# Create DataFrames
table_a = spark.createDataFrame(data_a, schema)
table_b = spark.createDataFrame(data_b, schema)

# --- Inner Join ---
inner_join_count = table_a.join(table_b, "ID").count()
print(f"Inner Join Count: {inner_join_count}")

# --- Left Join ---
left_join_count = table_a.join(table_b, "ID", "left").count()
print(f"Left Join Count: {left_join_count}")

# --- Right Join ---
right_join_count = table_a.join(table_b, "ID", "right").count()
print(f"right Join Count: {right_join_count}" )

# --- Full Outer Join ---
full_outer_join_count = table_a.join(table_b, "ID", "full").count()
print(f"Full Outer Join Count: {full_outer_join_count}")

#2nd highest age in SQL
#SELECT MAX(age) AS SecondHighestAge
#FROM employees
#WHERE age < (SELECT MAX(age) FROM employees);


transactions = [
    {"id": 1, "user": "A", "amount": 120, "type": "credit"},
    {"id": 2, "user": "B", "amount": 50, "type": "debit"},
    {"id": 3, "user": "A", "amount": 200, "type": "credit"},
    {"id": 4, "user": "C", "amount": 75, "type": "debit"},
    {"id": 5, "user": "B", "amount": 300, "type": "credit"},
    {"id": 6, "user": "A", "amount": 30, "type": "debit"}
]

# 1. Compute net balance per user
balances = {}
for transaction in transactions:
    user = transaction["user"]
    amount = transaction["amount"]
    trans_type = transaction["type"]

    if user not in balances:
        balances[user] = 0

    if trans_type == "credit":
        balances[user] += amount
    elif trans_type == "debit":
        balances[user] -= amount

# 2. Return the top 2 users by balance
top_users = sorted(balances.items(), key=lambda item: item[1], reverse=True)[:2]

print("User balances:", balances)
print("Top 2 users by balance:", top_users)

#prime no program

def is_prime(n):
    if n <= 1:
        return False
    return all(n % i != 0 for i in range(2, int(n**0.5) + 1))

# --- Examples ---
print(f"Is 29 prime? {is_prime(29)}")
print(f"Is 15 prime? {is_prime(15)}")