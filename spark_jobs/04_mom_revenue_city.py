from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as Fsum,
    trunc, to_date,
    lag, when, lit
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("04_mom_revenue_city")
    .getOrCreate()
)

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

base = (
    df.filter(col("city").isNotNull())
      .filter(col("sale_date").isNotNull())
      .filter(col("sale_dollars").isNotNull())
)

base = base.withColumn("month", to_date(trunc(col("sale_date"), "MM")))

monthly = (
    base.groupBy("city", "month")
        .agg(Fsum(col("sale_dollars")).alias("revenue"))
)

w = Window.partitionBy("city").orderBy(col("month"))

with_prev = monthly.withColumn("prev_revenue_raw", lag(col("revenue"), 1).over(w))

final_df = (
    with_prev
    .withColumn("prev_revenue", when(col("prev_revenue_raw").isNull(), lit(0.0)).otherwise(col("prev_revenue_raw")))
    .withColumn(
        "mom_pct",
        when(col("prev_revenue_raw").isNull(), lit(0.0))
        .when(col("prev_revenue_raw") == 0, lit(0.0))
        .otherwise((col("revenue") - col("prev_revenue_raw")) / col("prev_revenue_raw") * lit(100.0))
    )
    .select("city", "month", "revenue", "prev_revenue", "mom_pct")
)

print("SRC =", src)
print("Monthly rows =", final_df.count())
final_df.orderBy("city", "month").show(20, False)

(
    final_df.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "mom_revenue_city")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()