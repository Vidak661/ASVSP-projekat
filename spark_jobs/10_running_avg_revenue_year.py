from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum as Fsum, col, avg as Favg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("10_running_avg_revenue_year").getOrCreate()

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

rev_year = (
    df.filter(col("sale_date").isNotNull() & col("sale_dollars").isNotNull())
      .withColumn("year", year(col("sale_date")))
      .groupBy("year")
      .agg(Fsum("sale_dollars").alias("revenue"))
      .orderBy("year")
)

w = Window.orderBy("year").rowsBetween(-2, 0)  # poslednje 3 godine uključujući tekuću

out = rev_year.withColumn("avg_3y", Favg(col("revenue")).over(w)) \
              .select(col("year").cast("int"), col("revenue"), col("avg_3y"))

out.show(50, False)

(
    out.write.format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "running_avg_revenue_year")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()
