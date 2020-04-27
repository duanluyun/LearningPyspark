from pyspark.sql.functions import udf
from pyspark.sql import SparkSession

from pyspark.sql.types import *


if __name__ == '__main__':

    data=[
        ("a","1,2,3,4,5","1,3,4,5,6"),
        ("b", "1,2,3,4,5","1,3,5,3,2")
           ]


    spark=SparkSession.builder.appName("udf").master("local").getOrCreate()

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("v1", StringType(), True),
            StructField("v2", StringType(), True)

        ]
    )

    dataDF=spark.createDataFrame(data,schema)

    def count(s1,s2):
        s1=list(s1.split(","))
        s2=list(s2.split(","))
        n=0
        for i in s2:
            if i in s1:
                n+=1
        return n
    f1=udf(count,LongType())
    dataDF=dataDF.withColumn("count",f1(dataDF["v1"],dataDF["v2"]))
    dataDF.show()








