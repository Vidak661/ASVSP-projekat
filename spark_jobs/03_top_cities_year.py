from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum as Fsum, col, coalesce, trim, lit, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = (
    SparkSession.builder
    .appName("03_top_cities_year")
    .getOrCreate()
)

src = "hdfs://namenode:9000/data/transformed/iowa_liquor/liquor_sales_transformed"
df = spark.read.parquet(src)

base = (
    df.filter(col("sale_date").isNotNull())
      .filter(col("sale_dollars").isNotNull())
      .filter(col("bottles_sold").isNotNull())
      .withColumn("year", year("sale_date"))
      .withColumn("city_name", trim(coalesce(col("city"), lit("UNKNOWN"))))
)

agg = (
    base.groupBy("year", "city_name")
        .agg(
            Fsum(col("bottles_sold")).alias("total_bottles"),
            Fsum(col("sale_dollars")).alias("total_revenue"),
        )
)

w = Window.partitionBy("year").orderBy(desc("total_revenue"))

top5 = (
    agg.withColumn("rank", row_number().over(w))
       .filter(col("rank") <= 5)
       .select(
           col("year").cast("int").alias("year"),
           col("rank").cast("int").alias("rank"),
           col("city_name").alias("city"),
           col("total_bottles").cast("long").alias("total_bottles"),
           col("total_revenue").cast("double").alias("total_revenue"),
       )
       .orderBy("year", "rank")
)

# (opciono) debug da vidiš da nije prazno
print("SRC =", src)
print("TOP5 COUNT =", top5.count())
top5.show(50, False)

(
    top5.write
        .format("jdbc")
        .option("url", "jdbc:clickhouse://clickhouse:8123/asvsp")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "top_cities_year")
        .option("user", "asvsp")
        .option("password", "asvsp")
        .mode("append")  \
        .save()
)

spark.stop()