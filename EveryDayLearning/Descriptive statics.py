from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fn

spark=SparkSession.builder.appName('Descriptive statics').getOrCreate()
rdd=spark.sparkContext.textFile(r'C:\Users\Sam\Documents\Learning\Pyspark\data\ccFraud.csv')

header=rdd.first()

rdd=rdd.filter(lambda  row:row!=header).map(lambda row: [int(element) for element in row.split(',')])

cols=[c[1:-1] for c in header.split(",")]
structFieldList=[]

for col in cols:
    structFieldList.append(StructField(col,LongType(),True))

schema=StructType(structFieldList)
dataFrame=spark.createDataFrame(rdd,schema)

# --------------------------------------------------------------------------------------------观测性别不同时的分布-------------------------------------------------------------------------------
# dataFrame.groupBy('gender').count().show()
# --------------------------------------------------------------------------------------------对特定特征的统计性描述------------------------------------------------------------------------------
# dataFrame.printSchema()
# des=['balance','numTrans','numIntlTrans']
# dataFrame.describe(des).show()
#---------------------------------------------------------------------------------   观测特征的相似度----------------------------------------------------------------------------------------------------
print(dataFrame.corr('cardholder','balance'))
spark.stop()