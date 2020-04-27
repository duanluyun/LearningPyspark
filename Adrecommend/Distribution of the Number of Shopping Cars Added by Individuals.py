from pyspark.sql import SparkSession
from pyspark.sql.types import *



if __name__ == '__main__':
    data=([
        ("sam",20),
        ("sem", 30),
        ("tom", 40),
        ("tem", 50),
        ("anthony", 20),
        ("anthony2", 30),
        ("anthony3", 20)
    ])

    spark=SparkSession.builder.appName("distribution").master("local[*]").getOrCreate()
    rdd=spark.sparkContext.parallelize(data)

    schema = StructType(
        [
            StructField("custName", StringType(), True),
            StructField("goodSum", LongType(), True)

        ]
    )
    df = spark.createDataFrame(rdd, schema)
    df.show()
    df=df.groupBy("goodSum").count()
    df.createOrReplaceTempView("df")
    df=spark.sql("select goodSum as goods_sum,count as user_num from df")
    df.show()

    dict=dict(data)

    spark.stop()