from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

if __name__ == '__main__':
    spark=SparkSession.builder.appName("temp").getOrCreate()
    data=(
        ("1","https://m.ys.com/item/3954"),
        ("2", "https://m.ys.com/item/3955"),
        ("3", "https://m.ys.com/item/3956"),
        ("4", "https://m.ys.com/item/3956"),
        ("5", "https://m.ys.com/item/3954")

    )

    schema=StructType([
        StructField("id",StringType(),False),
        StructField("url", StringType(), False)
    ])

    dataFrame=spark.createDataFrame(data,schema)

    def check(x):
        if "https://m.ys.com/item/" in x:
            return 1
        else:
            return 0

    chec=udf(check,IntegerType())

    dataFrame=dataFrame.withColumn("check",chec(dataFrame["url"]))

    dataFrame2=dataFrame.groupBy("url").count()

    dataFrame.show()

    dataFrame2.show()
    spark.stop()