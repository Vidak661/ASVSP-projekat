from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("load_curated_to_clickhouse").getOrCreate()

# Izaberi izvor: curated (preporuka) ili transformed
SRC_PATH = "hdfs://namenode:9000/data/curated/iowa_liquor/liquor_sales_curated"
# SRC_PATH = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"

df = spark.read.parquet(SRC_PATH)

cols = [
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
]

df = df.select(*[c for c in cols if c in df.columns])

# Ako hoćeš prvo mali test:
# df = df.limit(10000).coalesce(1)

df = df.coalesce(1)

(
    df.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "liquor_sales_curated")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .option("batchsize", "5000")
    .mode("append")
    .save()
)

spark.stop()
