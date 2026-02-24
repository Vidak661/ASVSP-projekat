from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, sum as Fsum, row_number, desc, trim, coalesce, lit
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("02_top_items_year")
    .getOrCreate()
)

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

base = (
    df.filter(col("sale_date").isNotNull())
      .filter(col("item_number").isNotNull())
      .filter(col("sale_dollars").isNotNull())
      .filter(col("bottles_sold").isNotNull())
      .withColumn("year", year("sale_date"))
      .withColumn("item_name", trim(coalesce(col("item_description"), lit("UNKNOWN"))))
)

agg = (
    base.groupBy("year", "item_number", "item_name")
        .agg(
            Fsum(col("bottles_sold")).alias("total_bottles"),
            Fsum(col("sale_dollars")).alias("total_revenue"),
        )
)

w = Window.partitionBy("year").orderBy(desc("total_revenue"))

top5 = (
    agg.withColumn("rank", row_number().over(w))
       .filter(col("rank") <= 5)
       .select("year", "rank", "item_name", "total_bottles", "total_revenue")
       .orderBy("year", "rank")
)


(top5.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "top_items_year")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()
