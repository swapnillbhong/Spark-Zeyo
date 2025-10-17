
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


#Init example
#defination
#The __init__ method is the special
# set of instructions that runs automatically whenever
# you create a new object from that blueprint.

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

# Create an object (this calls __init__ automatically)
p1 = Person("Alice", 30)

# Access the attributes that were initialized
print(f"{p1.name} is {p1.age} years old.")

#project of most recent

#from pyspark.sql import SparkSession, Window

data = [
    (101, "2025-01-01", 8, "P001"),
    (102, "2025-01-01", 6, "P002"),
    (101, "2025-01-02", 7, "P005"),
    (103, "2025-01-01", 5, "P003"),
    (102, "2025-01-02", 8, "P002"),
    (101, "2025-01-03", 9, "P004")
]
columns = ["employee_id", "date", "hours_worked", "project_id"]
df = spark.createDataFrame(data, columns)

windowSpec = Window.partitionBy("employee_id").orderBy(col("date").desc())

recent_projects_df = df.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") == 1) \
    .select("employee_id", "date", "project_id")

print("Most recent project for each employee:")
recent_projects_df.show()