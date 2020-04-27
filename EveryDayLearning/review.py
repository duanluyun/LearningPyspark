from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import  *

data=(
    [
        (1, 143.5, 5.6, 33, 'M', 1000000),
        (2, 167.2, 5.4, 45, 'M', None),
        (3, None, 5.2, None, None, None),
        (4, 144.5, 5.9, 33, 'M', None),
        (5, 133.2, 5.7, 54, 'F', None),
        (6, 124.1, 5.2, None, 'F', None),
        (7, 129.2, 5.3, 42, 'M', 76000)
    ]
)

spark=SparkSession.builder.appName('review').getOrCreate()
rdd=spark.sparkContext.parallelize(data)

schema=StructType(
    [
StructField('id',LongType(),True),
StructField('height',FloatType(),True),
StructField('weight',FloatType(),True),
StructField('age',LongType(),True),
StructField('gender',StringType(),True),
StructField('income',StringType(),True)
    ]
)

dataFrame=spark.createDataFrame(rdd,schema)
dataFrame.show()

# -----------------------------------------------------------------------统计特征有缺失的记录------------------------------------------------------------------------------
print(dataFrame.rdd.map(lambda row:(row['id'],sum([c==None for c in row]))).collect())

#----------------------------------------------------------------------统计每个特征的缺失比例-----------------------------------------------------------------------
dataFrame.agg(
    *[
        (1-(fn.count(c)/fn.count('*'))).alias(c+'_missing') for c in dataFrame.columns
    ]
).show()

#-------------------------------------------------------------------剔除特征income---------------------------------------------------------------------------------
dataFrame=dataFrame.select([c for c in dataFrame.columns if c!='income'])
dataFrame.show()

#---------------------------------------------------------------剔除特征缺失过多的记录--------------------------------------------------------------------------
dataFrame=dataFrame.dropna(thresh=3)
dataFrame.show()

#-------------------------------------------------------------填充缺失值------------------------------------------------------------------------------------
means=dataFrame.agg(
    *[
        ( fn.mean(c)).alias(c) for c in dataFrame.columns if c!='gender'
    ]
).toPandas().to_dict('records')[0]

means['gender']='missing'
print('means is {0}'.format(means))

dataFrame=dataFrame.fillna(means)

dataFrame.show()
spark.stop()