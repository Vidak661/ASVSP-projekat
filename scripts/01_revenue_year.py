from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum as Fsum, col

spark = (SparkSession.builder
         .appName("01_revenue_year")
         .getOrCreate())

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

revenue_year = (
    df.filter(col("sale_date").isNotNull() & col("sale_dollars").isNotNull())
      .withColumn("year", year("sale_date"))
      .groupBy("year")
      .agg(Fsum("sale_dollars").alias("total_revenue"))
      .orderBy("year")
)

(revenue_year.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", "revenue_year")
    .option("user", "asvsp")
    .option("password", "asvsp")
    .mode("append")
    .save()
)

spark.stop()
