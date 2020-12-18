import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
sys.path.insert(0, '.')

if __name__ == "__main__":
    sc = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()
    # 读入用户信息以及商品信息
    user_df = sc.read.option("header",True).csv("in/user_info_format1.csv")
    item_df = sc.read.option("header",True).csv("in/user_log_format1.csv")
    #数据筛选，去重
    buyer_valid = item_df.filter((item_df.time_stamp == "1111") & (item_df.action_type == "2"))
    user_age_valid = user_df.filter((user_df.age_range != "0") | (user_df.age_range != ""))
    buyer_df = buyer_valid.select("user_id").distinct()
    # 统计各年龄段买家人数
    age_df = user_age_valid.join(buyer_df, on=["user_id"], how="inner")
    age_df_len = age_df.count()
    age_count = age_df.groupBy("age_range").count()
    # 转为pandas.DataFrame格式进行百分比计算
    pd_df = age_count.toPandas()
    pd_df['proportion'] = pd_df['count'] / age_df_len
    pd_df['proportion'] = pd_df['proportion'].apply(lambda x: '%.2f%%' % (x * 100))
    # 转回spark dataframe，输出结果
    result = sc.createDataFrame(pd_df)
    result.show()
