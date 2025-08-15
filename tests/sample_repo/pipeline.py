# SPARK_STAGE: 1,2
from pyspark.sql import SparkSession

def run_job(spark: SparkSession):
    df = spark.range(0, 100)
    # Simulate transformation
    result = df.filter("id > 10")
    # Collect small sample (OK)
    return result.take(5)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    print(run_job(spark))
    spark.stop()
