from pyspark.sql  import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fn

# -----------------------------------------------------------创建数据集---------------------------------------------------------------------------
data=(
    [
        (1,144.5,5.9,33,'M'),
        (2,167.2,5.4,45,'M'),
        (3,124.1,5.2,23,'F'),
        (4, 144.5, 5.9, 33, 'M'),
       (5,133.2,5.7,54,'F'),
        (3, 124.1, 5.2, 23, 'F'),
       (5,129.2,5.3,42,'M')
    ]
)

spark=SparkSession.builder.appName('dropDuplicates').getOrCreate()

schema=StructType(
    [
StructField('id',LongType(),False),
StructField('weight',FloatType(),False),
StructField('height',FloatType(),False),
StructField('age',LongType(),False),
StructField('gender',StringType(),False)
     ]
)

rdd=spark.sparkContext.parallelize(data)
dataFrame=spark.createDataFrame(rdd,schema)
# ----------------------------------------------------------删除完全相同的记录-------------------------------------------------------------
dataFrame.show()
print("count of  rows  {0}".format(dataFrame.count()))
print('count of distinct rows {0}'.format(dataFrame.distinct().count()))
dataFrame=dataFrame.dropDuplicates()

print("count of rows {0}".format(dataFrame.count()))
dataFrame.show()

# ------------------------------------------------------------删除除ID外完全相同的记录---------------------------------------------------------------------------------

dataFrame.createOrReplaceTempView('record')
print("count of  rows {0}".format(dataFrame.count()))
print("count of  rows which have distinct parameters except id {0}".format(dataFrame.select([c for c in dataFrame.columns if c!='id']).distinct().count()))
print("count of  rows which have distinct parameters except id {0}".format(
   spark.sql("select count(distinct(weight,height,age,gender)) from record").collect())
)
dataFrame=dataFrame.dropDuplicates(subset=[c for c in dataFrame.columns if c!='id'])
dataFrame.show()
print("count of  rows {0}".format(dataFrame.count()))
#--------------------------------------------------------------------除去ID相同的记录---------------------------------------------------------------------------------
dataFrame.show()
dataFrame.agg(
    fn.count('id').alias('count'),
    fn.countDistinct('id').alias('distinctCount'),

).show()
dataFrame=dataFrame.withColumn('new_id',fn.monotonically_increasing_id())
dataFrame.show()

spark.stop()