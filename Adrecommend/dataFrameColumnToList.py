from pyspark.sql import SparkSession
from pyspark.sql.types import  *



if __name__ == '__main__':
    spark=SparkSession.builder.appName("temp").master("local[*]").getOrCreate()

    data=(
        ("1","1"),
        ("2","2"),
        ("3","3")
    )

    schema=StructType([
        StructField("ID",StringType(),False),
        StructField("String",StringType(),False)
    ])

    dataDF=spark.createDataFrame(data,schema)

    ls=dataDF.select("String").rdd.map(lambda x: x[0]+"").collect()
    print(ls)

    spark.stop()