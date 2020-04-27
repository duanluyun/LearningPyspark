from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('udf').getOrCreate()

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def substract_mean(pdf):
    v = pdf.v
    return pdf.assign(v=v - v.mean())

df.groupby("id").apply(substract_mean).show()

