import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
sys.path.insert(0, '.')
import pandas as pd

if __name__ == "__main__":
    sc = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()
    # 读入用户信息以及商品信息
    user_df = sc.read.option("header",True).csv("in/user_info_format1.csv")
    item_df = sc.read.option("header",True).csv("in/user_log_format1.csv")
#    user_df.show(5)
#    item_df.show(5)
#    item_df.filter((item_df.time_stamp=="1111") & (item_df.action_type=="2")).show(10)
#    user_df.filter((user_df.gender=="0") | (user_df.gender=="1")).show(10)
    buyer_valid = item_df.filter((item_df.time_stamp=="1111") & (item_df.action_type=="2"))
    user_gender_valid = user_df.filter((user_df.gender=="0") | (user_df.gender=="1"))
    buyer_df = buyer_valid.select("user_id").distinct()
    # 统计男女比例
    gender_df = user_gender_valid.join(buyer_df, on=["user_id"], how="inner")
    gender_df_len = gender_df.count()
    gender_count = gender_df.groupBy("gender").count()
    # 转为pandas.DataFrame格式进行百分比计算
    pd_df = gender_count.toPandas()
    pd_df['proportion'] = pd_df['count'] / gender_df_len
    pd_df['proportion'] = pd_df['proportion'].apply(lambda x: '%.2f%%' % (x * 100))
    # 转回spark dataframe，输出结果
    result = sc.createDataFrame(pd_df)
    result.show()

