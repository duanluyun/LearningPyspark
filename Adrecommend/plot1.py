from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import when,count,isnull
from collections import defaultdict
import matplotlib.pyplot as plt
import  numpy as np
import matplotlib.font_manager as fm
font=fm.FontProperties(fname=r"C:\Windows\Fonts\STKAITI.TTF")

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

    df=spark.createDataFrame(rdd,schema)
    df.show()

# ----------------------------------------------------------统计值为Null的项--------------------------------------------------------------------------------

    null_Value=df.select([count(when(isnull(c),c)).alias(c) for c in df.columns]).toPandas()
    null_Value=dict(zip(null_Value.columns,null_Value.iloc[0].tolist()))
    print(null_Value)

    #-------------------------------------------------------- 统计值为“”的项----------------------------------------------------------------------------
    blank_value=defaultdict(int)
    for i in df.columns:
        blank_value[i]=df.select(i).filter(df[i]=='').count()
    print(blank_value)

    #---------------------------------------------------------- 统计总的空缺值-------------------------------------------------------------------------
    all_null_value=defaultdict(int)
    for i in blank_value.keys():
        all_null_value[i]=null_Value[i]+blank_value[i]

    all_null_value=dict(sorted(all_null_value.items(),key=lambda a:a[1],reverse=True))
    print(all_null_value)



    # --------------------------------------------------------------作图（Y轴柱状）-----------------------------------------------------------------------------
    plt.rcdefaults()
    fig,ax=plt.subplots(figsize=(4,8))
    titles=tuple(all_null_value.keys())
    titlesPos=np.arange(len(titles))
    ax.barh(titlesPos,tuple(all_null_value.values()),align='center',color='green',ecolor='black')
    ax.set_yticks(titlesPos)
    ax.set_yticklabels(titles)
    ax.invert_yaxis()
    ax.set_xlabel("缺失率",fontproperties=font,fontsize=20)
    ax.set_title("缺失", fontproperties=font,fontsize=20)
    plt.show()




    # -------------------------------------------------------------------作图2（X轴柱状）----------------------------------------------------------------
    plt.rcdefaults()
    plt.figure(figsize=(8, 6), dpi=80)
    plt.subplot(1, 1, 1)

    # 柱子总数
    N = len(tuple(all_null_value.keys()))
    # 包含每个柱子对应值的序列
    values = tuple(all_null_value.values())
    # 包含每个柱子下标的序列
    index = np.arange(N)
    #每个柱状的标签
    titles=tuple(all_null_value.keys())
    # 柱子的宽度
    width = 0.35
    # 绘制柱状图, 每根柱子的颜色为紫罗兰色
    p2 = plt.bar(index, values, width, label="rainfall", color="#87CEFA")
    # 设置横轴标签
    plt.xlabel('缺失率',fontproperties=font,fontsize=20)
    # 设置纵轴标签
    # plt.ylabel('rainfall (mm)')
    # 添加标题
    plt.title('缺失',fontproperties=font,fontsize=20)
    # 添加纵横轴的刻度
    plt.xticks(index,titles)
    plt.yticks(np.arange(0, 10, 1))
    # 添加图例
    plt.legend(loc="upper right")
    plt.show()
    spark.stop()