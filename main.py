from datetime import datetime, date

from delta import *
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
import yaml

def run(
    config: dict,
):

    deltapath = config["parameters"]["deltapath"]

    builder = (SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    if not DeltaTable.isDeltaTable(spark, f"{deltapath}/deltatable"):
        spark.sql(f"""
            CREATE TABLE deltatable(
                   a INT
                   ,b FLOAT
                   ,c STRING
                   ,d DATE
                   ,e TIMESTAMP
            ) USING DELTA
            LOCATION '{deltapath}'
        """)

    schema = """
       a INT
       ,b FLOAT
       ,c STRING
       ,d DATE
       ,e TIMESTAMP
    """

    spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ], schema=schema).write.format("delta").mode("overwrite").save(f"{deltapath}/deltatable")

    spark.createDataFrame([
        Row(a=5, b=6., c='string4', d=date(2000, 4, 1), e=datetime(2000, 1, 4, 12, 0)),
    ], schema=schema).write.format("delta").mode("append").save(f"{deltapath}/deltatable")

    deltaTable = DeltaTable.forPath(spark, f"{deltapath}/deltatable")

    df = deltaTable.toDF()

    print(df.dtypes)
    df.show()

    df = spark.read.format("delta") \
      .option("versionAsOf", 0) \
      .load(f"{deltapath}/deltatable")

    df.show()

if __name__ == "__main__":

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    run(config)
