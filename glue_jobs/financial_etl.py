from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark session
spark = SparkSession.builder.appName("FinancialETLPipeline").getOrCreate()

# Read raw financial transaction data from S3
df = spark.read.option("header", True).csv("s3://finance-raw-data/")

# Data cleaning and transformation
df_clean = (
    df.dropDuplicates(["transaction_id"])
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("transaction_date", col("transaction_date").cast("date"))
      .filter(col("amount").isNotNull())
      .withColumn(
          "amount_category",
          when(col("amount") > 10000, "HIGH").otherwise("NORMAL")
      )
)

# Write cleaned data to curated S3 bucket in Parquet format
df_clean.write.mode("overwrite").parquet("s3://finance-curated-data/")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark session
spark = SparkSession.builder.appName("FinancialETLPipeline").getOrCreate()

# Read raw financial transaction data from S3
df = spark.read.option("header", True).csv("s3://finance-raw-data/")

# Data cleaning and transformation
df_clean = (
    df.dropDuplicates(["transaction_id"])
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("transaction_date", col("transaction_date").cast("date"))
      .filter(col("amount").isNotNull())
      .withColumn(
          "amount_category",
          when(col("amount") > 10000, "HIGH").otherwise("NORMAL")
      )
)

# Write cleaned data to curated S3 bucket in Parquet format
df_clean.write.mode("overwrite").parquet("s3://finance-curated-data/")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark session
spark = SparkSession.builder.appName("FinancialETLPipeline").getOrCreate()

# Read raw financial transaction data from S3
df = spark.read.option("header", True).csv("s3://finance-raw-data/")

# Data cleaning and transformation
df_clean = (
    df.dropDuplicates(["transaction_id"])
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("transaction_date", col("transaction_date").cast("date"))
      .filter(col("amount").isNotNull())
      .withColumn(
          "amount_category",
          when(col("amount") > 10000, "HIGH").otherwise("NORMAL")
      )
)

# Write cleaned data to curated S3 bucket in Parquet format
df_clean.write.mode("overwrite").parquet("s3://finance-curated-data/")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark session
spark = SparkSession.builder.appName("FinancialETLPipeline").getOrCreate()

# Read raw financial transaction data from S3
df = spark.read.option("header", True).csv("s3://finance-raw-data/")

# Data cleaning and transformation
df_clean = (
    df.dropDuplicates(["transaction_id"])
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("transaction_date", col("transaction_date").cast("date"))
      .filter(col("amount").isNotNull())
      .withColumn(
          "amount_category",
          when(col("amount") > 10000, "HIGH").otherwise("NORMAL")
      )
)

# Write cleaned data to curated S3 bucket in Parquet format
df_clean.write.mode("overwrite").parquet("s3://finance-curated-data/")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark session
spark = SparkSession.builder.appName("FinancialETLPipeline").getOrCreate()

# Read raw financial transaction data from S3
df = spark.read.option("header", True).csv("s3://finance-raw-data/")

# Data cleaning and transformation
df_clean = (
    df.dropDuplicates(["transaction_id"])
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("transaction_date", col("transaction_date").cast("date"))
      .filter(col("amount").isNotNull())
      .withColumn(
          "amount_category",
          when(col("amount") > 10000, "HIGH").otherwise("NORMAL")
      )
)

# Write cleaned data to curated S3 bucket in Parquet format
df_clean.write.mode("overwrite").parquet("s3://finance-curated-data/")
