from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import *

#Initializing Spark Session
spark = SparkSession.builder \
    .master('local') \
    .appName('data_cleaner') \
    .getOrCreate()

#Getting spark context
sc = spark.sparkContext
sc.setLogLevel('WARN')

#Defining Schema
schema = StructType([\
    StructField('order_id', IntegerType(), True), \
    StructField('customer_id', IntegerType(), True), \
    StructField('customer_name', StringType(), True), \
    StructField('product_id', IntegerType(), True), \
    StructField('product_name', StringType(), True), \
    StructField('product_category', StringType(), True), \
    StructField('payment_type', StringType(), True), \
    StructField('qty', IntegerType(), True), \
    StructField('price', FloatType(), True), \
    StructField('datetime', StringType(), True), \
    StructField('country', StringType(), True), \
    StructField('city', StringType(), True), \
    StructField('ecommerce_website_name', StringType(), True), \
    StructField('payment_txn_id', IntegerType(), True), \
    StructField('payment_txn_success', StringType(), True), \
    StructField('failure_reason', StringType(), True)
    ])

#loading data into dataframe
df = spark.read \
    .format('csv') \
    .option('inferschema', 'true') \
    .schema(schema) \
    .load('/home/michael/data_team_4.csv')


#dropping rows with null values (except failure_reason)
df = df.filter('datetime != "00000000000"')
for col in df.columns:
    if(col != 'failure_reason'): df = df.filter(df[col].isNotNull())

#splitting date into date and time, as well as an hour column for convenience in power BI
split_date = split(df['datetime'], ' ')

df = df.withColumn('date', split_date.getItem(0))
df = df.withColumn('time', split_date.getItem(1))
df = df.drop('datetime')

split_time = split(df['time'], ':')
df = df.withColumn('hour', split_time.getItem(0))


#Splitting product category
split_category = split(df['product_category'], ',')
df = df.withColumn('product_category_1', split_category.getItem(0))
df = df.withColumn('product_category_2', split_category.getItem(1))
df = df.drop('product_category')


#Creating 'total' column
df = df.withColumn('total', df['qty']*df['price'])

df.explain(True)

df.write.mode('overwrite').option('header', 'true').csv('/home/michael/cleaned_data')