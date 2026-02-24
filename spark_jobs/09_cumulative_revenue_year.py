from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum as Fsum, col
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as Wsum

spark = SparkSession.builder.appName("09_cumulative_revenue_year").getOrCreate()

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

rev_year = (
    df.filter(col("sale_date").isNotNull() & col("sale_dollars").isNotNull())
      .withColumn("year", year(col("sale_date")))
      .groupBy("year")
      .agg(Fsum("sale_dollars").alias("revenue"))
      .orderBy("year")
)

w = Window.orderBy("year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

out = rev_year.withColumn("cumulative_revenue", Wsum(col("revenue")).over(w)) \
              .select(col("year").cast("int"), col("revenue"), col("cumulative_revenue"))

out.show(50, False)

(
    out.write.format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "cumulative_revenue_year")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()