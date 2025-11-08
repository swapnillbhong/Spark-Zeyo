from pyspark.sql import SparkSession

# 1️⃣ Create Spark session
spark = SparkSession.builder.appName("AvgSalaryByDept").getOrCreate()

# 2️⃣ Create RDD from list
rdd = spark.sparkContext.parallelize([
    (1, "HR", 3000),
    (2, "IT", 3500),
    (3, "admin", 4000),
    (4, "HR", 5000),
    (5, "admin", 6500)
])

# 3️⃣ Map to (department, (salary, 1)) pairs
dept_salary_pairs = rdd.map(lambda x: (x[1], (x[2], 1)))

# 4️⃣ Reduce by key → sum salaries and counts per department
dept_totals = dept_salary_pairs.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# 5️⃣ Compute average = total_salary / total_count
dept_avg = dept_totals.mapValues(lambda x: x[0] / x[1])

# 6️⃣ Show results
for dept, avg in dept_avg.collect():
    print(f"{dept}: {avg}")

spark.stop()