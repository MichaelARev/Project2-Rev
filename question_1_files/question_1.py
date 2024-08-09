# Creates tables from a cleaned .csv to answer 'What is the top selling category of items? Per country?'

from pyspark.sql import SparkSession

# Build sparkSession object
spark = SparkSession.builder \
    .master("local") \
    .appName("project_2_question_1") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load('file:///mnt/c/Users/bxt72/Documents/data_team_4_cleaned.csv')

df.createOrReplaceTempView("transactions_df")

sales_by_category = spark.sql('SELECT product_category AS Category, ROUND(SUM(price), 2) AS Sales, COUNT(order_id) AS Units_Sold \
           FROM transactions_df \
           GROUP BY product_category') 
        
sales_by_country_and_category = spark.sql('SELECT product_category AS Category, country, ROUND(SUM(price), 2) AS Sales, COUNT(order_id) AS Units_Sold \
           FROM transactions_df \
           GROUP BY product_category, country') 
        
sales_by_category.write.format("csv").save("/home/bxt7280/test1")
sales_by_country_and_category.write.format("csv").save("/home/bxt7280/test2")