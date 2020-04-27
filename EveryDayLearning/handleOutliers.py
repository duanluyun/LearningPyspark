from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fn

data=(
    [
(1,143.5,5.3,28),
(2,154.2,5.5,45),
(3,342.3,5.1,99),
(4,144.5,5.5,33),
(5,133.2,5.4,54),
(6,124.1,5.1,21),
(7,129.2,5.3,42),
    ]
)

spark=SparkSession.builder.appName('handleOutLiers').getOrCreate()
rdd=spark.sparkContext.parallelize(data)

schema=StructType([
StructField('id',LongType(),True),
StructField('weight',FloatType(),True),
StructField('height',FloatType(),True),
StructField('age',LongType(),True)
])

dataFrame=spark.createDataFrame(rdd,schema)
dataFrame.show()

# ----------------------------------------------------------------------------------------计算数据集的上界和下界----------------------------------------------------------------------------
cols=['weight','height','age']
bounds={}
for col in cols:
    temp=dataFrame.approxQuantile(col,[0.25,0.75],0.05)
    IQR=temp[1]-temp[0]
    bounds[col]=[temp[0]-1.5*IQR,temp[1]+1.5*IQR]
print(bounds)

#-------------------------------------------------------------------------------------------查看数据集中是否含有离群值-----------------------------------------------------------------------------------------------------------
outLiers=dataFrame.select(*
    ['id']+[
        (  (dataFrame[c]<bounds[c][0]) |
        (dataFrame[c]>bounds[c][1])).alias(c+'_outLier')  for c in cols
    ]
)
# -----------------------------------------------------------------------------------------------查找离群值-------------------------------------------------------------------------
dataFrame=dataFrame.join(outLiers,on='id')
dataFrame.show()
dataFrame.filter('weight_outlier').select(['id','weight']).show()
dataFrame.filter('age_outlier').select(['id','age']).show()
spark.stop()