from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import when,count,isnull
from collections import defaultdict


if __name__ == '__main__':
    spark = SparkSession.builder.appName("MissingValueAnalysis").master("local[*]").getOrCreate()

    data = [
        (1, 2, 3, 4, 5, None),
        (1, 2, 3, 4, None, None),
        (1, 2, None, None, 5, None),
        (1, 2, None, '', None, None)
    ]

    rdd = spark.sparkContext.parallelize(data)
    schema=StructType(
        [
            StructField("BoolCin",StringType(),True),
            StructField("BoolCi", StringType(), True),
            StructField("BoolC", StringType(), True),
            StructField("Bool", StringType(), True),
            StructField("Boo", StringType(), True),
            StructField("Bo", StringType(), True),
        ]
    )

    dataframe=spark.createDataFrame(rdd,schema)
    dataframe.show()

# ----------------------------------------------------------统计值为Null的项--------------------------------------------------------------------------------

    null_Value=dataframe.select([count(when(isnull(c),c)).alias(c) for c in dataframe.columns]).toPandas()
    null_Value=dict(zip(null_Value.columns,null_Value.iloc[0].tolist()))


    #-------------------------------------------------------- 统计值为“”的项----------------------------------------------------------------------------
    blank_value=defaultdict(int)
    for i in dataframe.columns:
        blank_value[i]=dataframe.select(i).filter(dataframe[i]=='').count()


    #---------------------------------------------------------- 统计总的空缺值-------------------------------------------------------------------------
    all_null_value=defaultdict(int)
    for i in blank_value.keys():
        all_null_value[i]=null_Value[i]+blank_value[i]

    all_null_value=dict(sorted(all_null_value.items(),key=lambda a:a[1],reverse=True))

    spark.stop()

