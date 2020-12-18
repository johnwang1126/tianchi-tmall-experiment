import sys
import pandas as pd
from pyspark.sql import SparkSession
sys.path.insert(0, '.')

sc = SparkSession.builder.master("local[*]").appName("SparkML-example.com").getOrCreate()
# load data
df_train = sc.read.option("header",True).csv("in/train_format1.csv")
df_test = sc.read.option("header",True).csv("in/test_format1.csv")

user_info = sc.read.option("header",True).csv("in/user_info_format1.csv")
user_log = sc.read.option("header",True).csv("in/user_log_format1.csv")

#%%
# age, gender
# df_train.show(10)
df_train = df_train.join(user_info, on="user_id", how="left")
# df_train.show(10)
pd_df_train = df_train.toPandas()

#%%
# total_logs:某用户在该商家日志的总条数
total_logs_temp = user_log.groupBy("user_id", "seller_id").count()
total_logs_temp = total_logs_temp.withColumnRenamed("count", "total_logs")
total_logs_temp = total_logs_temp.withColumnRenamed("seller_id", "merchant_id")
pd_total_logs_temp = total_logs_temp.toPandas()
#total_logs_temp.show(10)
#df_train = df_train.join(total_logs_temp, on=["user_id", "merchant_id"], how="left")
#df_train.show(10)

#%%
# unique_item_ids:用户浏览的商品的数目，就是浏览了多少个商品
unique_item_ids_temp = user_log.groupBy("user_id", "seller_id", "item_id").count()
#unique_item_ids_temp.show(10)
unique_item_ids_temp1 = unique_item_ids_temp.groupBy("user_id", "seller_id").count()
unique_item_ids_temp1 = unique_item_ids_temp1.withColumnRenamed("seller_id", "merchant_id")
unique_item_ids_temp1 = unique_item_ids_temp1.withColumnRenamed("count", "unique_item_ids")
#unique_item_ids_temp1.show(10)
#df_train = df_train.join(unique_item_ids_temp1, on=["user_id", "merchant_id"], how="left")
#df_train.show(10)

#%%
# categories:浏览的商品的种类的数目，就是浏览了多少种商品
categories_temp = user_log.groupBy("user_id", "seller_id", "cat_id").count()
#categories_temp.show(10)
categories_temp1 = categories_temp.groupBy("user_id", "seller_id").count()
#categories_temp1.show(10)
categories_temp1 = categories_temp1.withColumnRenamed("seller_id", "merchant_id")
categories_temp1 = categories_temp1.withColumnRenamed("count", "categories")
df_train = df_train.join(categories_temp1, on=["user_id", "merchant_id"], how="left")
df_train.show(10)

#%%
# browse_days：用户浏览的天数
browse_days_temp = user_log.groupBy("user_id", "seller_id", "time_stamp").count()
#browse_days_temp.show(10)
browse_days_temp1 = browse_days_temp.groupBy("user_id", "seller_id").count()
#browse_days_temp1.show(10)
browse_days_temp1 = browse_days_temp1.withColumnRenamed("seller_id", "merchant_id")
browse_days_temp1 = browse_days_temp1.withColumnRenamed("count", "browse_days")
df_train = df_train.join(browse_days_temp1, on=["user_id", "merchant_id"], how="left")
df_train.show(10)

#%%
# 用户单击的次数(one_clicks)
# 用户添加购物车的次数(shopping_carts)
# 用户购买的次数(purchase_times)
# 用户收藏的次数(favourite_times)
one_clicks_temp = user_log.groupBy("user_id", "seller_id", "action_type").count()
one_clicks_temp = one_clicks_temp.withColumnRenamed("seller_id", "merchant_id")
one_clicks_temp = one_clicks_temp.withColumnRenamed("count", "times")
one_clicks_temp.show(10)
#%%
pandas_one_clicks_temp = one_clicks_temp.toPandas()

