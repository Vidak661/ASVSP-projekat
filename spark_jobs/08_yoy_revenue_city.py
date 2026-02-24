from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, sum as Fsum, col,
    trim, coalesce, lit,
    lag, when
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("08_yoy_revenue_city")
    .getOrCreate()
)

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

base = (
    df.filter(col("sale_date").isNotNull())
      .filter(col("sale_dollars").isNotNull())
      .withColumn("year", year(col("sale_date")))
      .withColumn(
          "city_clean",
          trim(coalesce(col("city"), lit("UNKNOWN")))
      )
)

agg = (
    base.groupBy("city_clean", "year")
        .agg(Fsum(col("sale_dollars")).alias("revenue"))
)

w = Window.partitionBy("city_clean").orderBy("year")

with_prev = agg.withColumn(
    "prev_revenue_raw",
    lag(col("revenue"), 1).over(w)
)

final_df = (
    with_prev
    .withColumn(
        "prev_revenue",
        when(col("prev_revenue_raw").isNull(), lit(0.0))
        .otherwise(col("prev_revenue_raw"))
    )
    .withColumn(
        "yoy_pct",
        when(col("prev_revenue_raw").isNull(), lit(0.0))
        .when(col("prev_revenue_raw") == 0, lit(0.0))
        .otherwise((col("revenue") - col("prev_revenue_raw")) / col("prev_revenue_raw") * 100.0)
    )
    .select(
        col("city_clean").alias("city"),
        col("year"),
        col("revenue"),
        col("prev_revenue"),
        col("yoy_pct")
    )
    .orderBy("city", "year")
)

print("Rows =", final_df.count())
final_df.show(30, False)

(
    final_df.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "yoy_revenue_city")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()