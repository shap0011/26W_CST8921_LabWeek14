import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\olga_\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\olga_\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("Big Data Analytics Lab") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# DATASET — Simulated retail transactions across regions
transactions = [
    (1,  "T001", "Alice",   "North", "Electronics", 899.99, 2, "2024-01-05 10:30:00", "credit_card"),
    (2,  "T002", "Bob",     "South", "Clothing",     45.00, 3, "2024-01-06 11:00:00", "cash"),
    (3,  "T003", "Charlie", "East",  "Electronics", 199.50, 1, "2024-01-06 14:20:00", "debit_card"),
    (4,  "T004", "Alice",   "North", "Food",          12.50, 5, "2024-01-07 09:15:00", "cash"),
    (5,  "T005", "David",   "West",  "Electronics", 450.00, 1, "2024-01-08 16:45:00", "credit_card"),
    (6,  "T006", "Eve",     "South", "Food",          22.00, 4, "2024-01-08 18:00:00", "credit_card"),
    (7,  "T007", "Frank",   "North", "Clothing",     75.00, 2, "2024-01-09 13:30:00", "debit_card"),
    (8,  "T008", "Grace",   "East",  "Food",          33.00, 3, "2024-01-10 10:00:00", "cash"),
    (9,  "T009", "Heidi",   "West",  "Electronics", 600.00, 1, "2024-02-01 12:00:00", "credit_card"),
    (10, "T010", "Ivan",    "South", "Clothing",    110.00, 2, "2024-02-02 15:30:00", "debit_card"),
    (11, "T011", "Alice",   "North", "Electronics", 250.00, 1, "2024-02-03 09:00:00", "credit_card"),
    (12, "T012", "Bob",     "South", "Food",         18.00, 6, "2024-02-04 17:00:00", "cash"),
    (13, "T013", "Charlie", "East",  "Clothing",     95.00, 1, "2024-02-05 11:45:00", "credit_card"),
    (14, "T014", "David",   "West",  "Food",          8.50, 2, "2024-02-06 08:30:00", "debit_card"),
    (15, "T015", "Eve",     "South", "Electronics", 320.00, 1, "2024-02-07 14:00:00", "credit_card"),
    (16, "T016", "Frank",   "North", "Food",         55.00, 3, "2024-03-01 10:15:00", "cash"),
    (17, "T017", "Grace",   "East",  "Electronics", 780.00, 2, "2024-03-02 16:00:00", "credit_card"),
    (18, "T018", "Heidi",   "West",  "Clothing",    200.00, 1, "2024-03-03 12:30:00", "debit_card"),
    (19, "T019", "Ivan",    "South", "Food",         40.00, 5, "2024-03-04 09:45:00", "cash"),
    (20, "T020", "Alice",   "North", "Electronics", 999.99, 1, "2024-03-05 11:00:00", "credit_card"),
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("region", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
])

#=============================================================
# Step 6: Convert `timestamp` and add `revenue`
#=============================================================

# Create DataFrame
df = spark.createDataFrame(transactions, schema=schema)

# Convert timestamp to proper datetime type
df = df.withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Add revenue column
df = df.withColumn("revenue", F.col("unit_price") * F.col("quantity"))

# Show schema and sample data
df.printSchema()
df.show(5, truncate=False)

#=============================================================
# Step 7: Descriptive analytics
#=============================================================

print("\n=== Summary Statistics ===")
df.select("unit_price", "quantity", "revenue").describe().show()

print("\n=== Revenue by Category ===")
df.groupBy("category") \
    .agg(F.round(F.sum("revenue"), 2).alias("total_revenue")) \
    .orderBy(F.desc("total_revenue")) \
    .show()

print("\n=== Revenue by Region ===")
df.groupBy("region") \
    .agg(F.round(F.sum("revenue"), 2).alias("total_revenue")) \
    .orderBy(F.desc("total_revenue")) \
    .show()

#=============================================================
# Step 8: Diagnostic analytics
#=============================================================
    
print("\n=== Revenue by Region and Category ===")
df.groupBy("region", "category") \
    .agg(F.round(F.sum("revenue"), 2).alias("total_revenue")) \
    .orderBy("region", F.desc("total_revenue")) \
    .show()

print("\n=== Pivot Table: Revenue by Payment Method and Category ===")
df.groupBy("payment_method") \
    .pivot("category") \
    .agg(F.round(F.sum("revenue"), 2)) \
    .show()

print("\n=== Monthly Revenue Trends ===")
df.withColumn("month", F.date_format("timestamp", "yyyy-MM")) \
    .groupBy("month") \
    .agg(F.round(F.sum("revenue"), 2).alias("total_revenue")) \
    .orderBy("month") \
    .show()
    
#=============================================================
# Step 9: Advanced analytics with window functions
#=============================================================

print("\n=== Customer Revenue Ranking ===")
customer_window = Window.orderBy(F.desc("total_revenue"))

customer_revenue = df.groupBy("customer") \
    .agg(F.round(F.sum("revenue"), 2).alias("total_revenue")) \
    .withColumn("rank", F.rank().over(customer_window))

customer_revenue.show()

print("\n=== Running Revenue by Region Over Time ===")
running_window = Window.partitionBy("region").orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_df = df.select("region", "timestamp", "transaction_id", "revenue") \
    .withColumn("running_revenue", F.round(F.sum("revenue").over(running_window), 2)) \
    .orderBy("region", "timestamp")

running_df.show(truncate=False)

print("\n=== Revenue Quartiles ===")
quartile_window = Window.orderBy(F.desc("revenue"))

df_quartiles = df.select("transaction_id", "customer", "revenue") \
    .withColumn("quartile", F.ntile(4).over(quartile_window))

df_quartiles.show()

#=============================================================
# Step 10: RFM Scoring (Predictive-style analytics)
#=============================================================

print("\n=== RFM Scoring ===")

# Reference date (latest date in dataset)
max_date = df.agg(F.max("timestamp")).collect()[0][0]

# Calculate RFM metrics
rfm = df.groupBy("customer").agg(
    F.datediff(F.lit(max_date), F.max("timestamp")).alias("recency"),
    F.count("transaction_id").alias("frequency"),
    F.round(F.sum("revenue"), 2).alias("monetary")
)

# Create scoring buckets (1–4)
rfm = rfm.withColumn("R_score", F.ntile(4).over(Window.orderBy(F.col("recency").desc()))) \
         .withColumn("F_score", F.ntile(4).over(Window.orderBy(F.col("frequency")))) \
         .withColumn("M_score", F.ntile(4).over(Window.orderBy(F.col("monetary"))))

# Combine scores
rfm = rfm.withColumn("RFM_score",
                     F.concat(F.col("R_score"), F.col("F_score"), F.col("M_score")))

rfm.show()

#=============================================================
# Step 11: Customer Segmentation
#=============================================================

print("\n=== Customer Segmentation ===")

rfm_segmented = rfm.withColumn(
    "segment",
    F.when(F.col("RFM_score") == "444", "Champions")
     .when(F.col("R_score") >= 3, "Loyal Customers")
     .when(F.col("R_score") == 2, "Potential Loyalists")
     .otherwise("At Risk")
)

rfm_segmented.select("customer", "RFM_score", "segment").show()

#=============================================================
# Step 12: Anomaly Detection using z-scores
#=============================================================

print("\n=== Anomaly Detection (Z-Score) ===")

stats = df.select(
    F.mean("revenue").alias("mean_revenue"),
    F.stddev("revenue").alias("stddev_revenue")
).collect()[0]

mean_revenue = stats["mean_revenue"]
stddev_revenue = stats["stddev_revenue"]

df_anomaly = df.withColumn(
    "z_score",
    F.round((F.col("revenue") - F.lit(mean_revenue)) / F.lit(stddev_revenue), 2)
).withColumn(
    "is_anomaly",
    F.when(F.abs(F.col("z_score")) > 2, "Yes").otherwise("No")
)

df_anomaly.select(
    "transaction_id", "customer", "region", "category", "revenue", "z_score", "is_anomaly"
).orderBy(F.desc("z_score")).show(truncate=False)

#=============================================================
# Step 13: Save output to Parquet (Final step)
#=============================================================

print("\n=== Saving Data using Pandas (Final Working Solution) ===")

# Convert Spark DataFrame to Pandas
pdf = df.toPandas()

# Save as CSV
output_path = "output/final_transactions.csv"
pdf.to_csv(output_path, index=False)

print(f"Data successfully saved to {output_path}")