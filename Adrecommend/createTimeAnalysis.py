import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime,year,month,hour,weekofyear,date_format
from pyspark.sql.functions import  from_unixtime



if __name__ == '__main__':


    spark=SparkSession.builder.appName("createTime Analyze").master("local[*]").getOrCreate()

    data=(
        [
            (1,1519401161),
            (2, 1519401635),
            (3, 1519401642),
            (4, 1519401653),
            (5, 1519401657)
        ]
    )

    schema=StructType([
        StructField("id",LongType(),False),
        StructField("createTime",LongType(),False)
    ])

    dataFrame=spark.createDataFrame(data,schema)



    dataFrame=dataFrame.withColumn("createTimeStamp",from_unixtime("createTime").cast(TimestampType())).\
        withColumn("year",year("createTimeStamp")).\
        withColumn("month",month("createTimeStamp")).\
        withColumn("hour",hour("createTimeStamp")). \
        withColumn("Date", from_unixtime("createTime").cast(DateType())).\
        withColumn("createTimeWeeks",date_format(from_unixtime("createTime"),'u')).\
        withColumn("YYYY-MM",from_unixtime("createTime",'yyyy-mm'))


    dataFrame.show()

    spark.stop()