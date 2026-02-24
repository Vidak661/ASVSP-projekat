from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("load_liquor_to_clickhouse").getOrCreate()

path = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(path)


df = df.select(
    "invoice_item_number",
    "sale_date",
    "store_number",
    "city",
    "county_number",
    "county",
    "category",
    "category_name",
    "vendor_number",
    "vendor_name",
    "item_number",
    "item_description",
    "pack",
    "bottle_volume_ml",
    "state_bottle_cost",
    "state_bottle_retail",
    "bottles_sold",
    "sale_dollars",
    "volume_sold_liters",
    "volume_sold_gallons",
)

df = df.filter(col("sale_date").isNotNull() & col("store_number").isNotNull() & col("item_number").isNotNull())
df = df.limit(10000)
df = df.coalesce(1)

(df.write
 .format("jdbc")
 .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp?socket_timeout=600000&connect_timeout=60000")
 .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
 .option("dbtable", "liquor_sales_curated")
 .option("user", "asvsp")
 .option("password", "asvsp")
 .option("batchsize", "2000")
 .option("numPartitions", "1")
 .mode("append")
 .save()
)

spark.stop()
