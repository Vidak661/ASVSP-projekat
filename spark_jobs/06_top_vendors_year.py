from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum as Fsum, col, trim, coalesce, lit, desc, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("06_top_vendors_year")
    .getOrCreate()
)

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

base = (
    df.filter(col("sale_date").isNotNull())
      .filter(col("sale_dollars").isNotNull())
      .filter(col("bottles_sold").isNotNull())
      .withColumn("year", year(col("sale_date")))
      .withColumn("vendor_clean", trim(coalesce(col("vendor_name"), lit("UNKNOWN"))))
)

agg = (
    base.groupBy("year", "vendor_clean")
        .agg(
            Fsum(col("sale_dollars")).alias("total_revenue"),
            Fsum(col("bottles_sold")).alias("total_bottles"),
        )
)

w = Window.partitionBy("year").orderBy(desc("total_revenue"))

top10 = (
    agg.withColumn("rank", row_number().over(w))
       .filter(col("rank") <= 10)
       .select(
           col("year").cast("int").alias("year"),
           col("rank").cast("int").alias("rank"),
           col("vendor_clean").alias("vendor"),
           col("total_revenue"),
           col("total_bottles").cast("long").alias("total_bottles"),
       )
       .orderBy("year", "rank")
)

print("SRC =", src)
print("Top10 rows =", top10.count())
top10.show(50, False)

(
    top10.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "top_vendors_year")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()