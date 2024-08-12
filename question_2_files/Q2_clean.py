from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import mean, stddev, col, when

spark = SparkSession.builder  \
     .master("local")  \
     .appName('df_intro')\
     .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

df_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("datetime", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ecom_site", StringType(), True),
    StructField("txn_id", StringType(), True),
    StructField("txn_s", StringType(), True),
    StructField("failure_reason", StringType(), True)
])

df = spark.read \
    .format("csv") \
    .option("header", "false") \
    .schema(df_schema) \
    .load("/home/john/data_team_4.csv")


#Drops all rows with missing data in any column except for the failure_reason column
df = df.dropna(subset=[col for col in df.columns if col != "failure_reason"])

#Drops all rows with values of 00000000000 in the datetime column
df = df.filter(df.datetime != '00000000000')

#Drop Unnecessary Columns for Question 2
df = df.drop("customer_id", "customer_name", "payment_type", "city", "ecom_site", "txn_id", "txn_s", "failure_reason")

#Remove Outliers from quantity
mean_val = df.select(mean(col('qty'))).collect()[0][0]
stddev_val = df.select(stddev(col('qty'))).collect()[0][0]
lower_bound = mean_val - (3 * stddev_val)
upper_bound = mean_val + (3 * stddev_val)
df_no_outliers_stddev = df.filter((col('qty') >= lower_bound) & (col('qty') <= upper_bound))

#Remove Outliers from price
mean_val = df.select(mean(col('price'))).collect()[0][0]
stddev_val = df.select(stddev(col('price'))).collect()[0][0]
lower_bound = mean_val - (3 * stddev_val)
upper_bound = mean_val + (3 * stddev_val)
df_no_outliers_stddev = df.filter((col('price') >= lower_bound) & (col('price') <= upper_bound))



#Change Values in the product_category column if they are 2 values
df = df.withColumn(
    "product_category",
    when(col("product_category") == "Garden, Plants", "Garden")
    .when(col("product_category") == "Wood, Garden", "Garden")
    .when(col("product_category") == "Pot, Plants", "Garden")
    .when(col("product_category") == "Plants", "Garden")
    .when(col("product_category") == "Copper, Bedroom", "Bedroom")
    .when(col("product_category") == "Black, Leather", "Chair")
    .when(col("product_category") == "Couch", "Sofa")
    .when(col("product_category") == "Wood, Bedroom", "Bedroom")
    .when(col("product_category") == "Antique, Bedroom", "Bedroom")
    .when(col("product_category") == "Couch, Wood", "Sofa")
    .when(col("product_category") == "Garden, Wood", "Garden")
    .when(col("product_category") == "Pillows", "Bedroom")
    .when(col("product_category") == "Bed", "Bedroom")
    .otherwise(col("product_category"))
)




df.write.csv("/home/john/Clean.csv", header=True)

