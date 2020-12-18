#%%
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '.')

# load data
df_train = pd.read_csv("in/train_format1.csv")
df_test = pd.read_csv("in/test_format1.csv")
user_info = pd.read_csv("in/user_info_format1.csv")
user_log = pd.read_csv("in/user_log_format1.csv")
#%%
# 缺失值预处理
user_info['age_range'].replace(0.0,np.nan,inplace=True)
user_info['gender'].replace(2.0,np.nan,inplace=True)
user_info['age_range'].replace(np.nan,-1,inplace=True)
user_info['gender'].replace(np.nan,-1,inplace=True)
user_info['age_range'].replace(-1,np.nan,inplace=True)
user_info['gender'].replace(-1,np.nan,inplace=True)
#%%
# age_range, gender
df_train = pd.merge(df_train,user_info,on="user_id",how="left")

#%%
# total_logs
total_logs_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"]]).count().\
    reset_index()[["user_id","seller_id","item_id"]]
total_logs_temp.rename(columns={"seller_id":"merchant_id","item_id":"total_logs"},inplace=True)
df_train = pd.merge(df_train,total_logs_temp,on=["user_id","merchant_id"],how="left")

#%%
# unique_item_id
unique_item_ids_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["item_id"]]).count()\
    .reset_index()[["user_id","seller_id","item_id"]]
unique_item_ids_temp1 = unique_item_ids_temp.groupby([unique_item_ids_temp["user_id"],unique_item_ids_temp["seller_id"]])\
    .count().reset_index()
unique_item_ids_temp1.rename(columns={"seller_id":"merchant_id","item_id":"unique_item_ids"},inplace=True)
df_train = pd.merge(df_train,unique_item_ids_temp1,on=["user_id","merchant_id"],how="left")

#%%
# categories
categories_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["cat_id"]]).count()\
    .reset_index()[["user_id","seller_id","cat_id"]]
categories_temp1 = categories_temp.groupby([categories_temp["user_id"],categories_temp["seller_id"]])\
    .count().reset_index()
categories_temp1.rename(columns={"seller_id":"merchant_id","cat_id":"categories"},inplace=True)
df_train = pd.merge(df_train,categories_temp1,on=["user_id","merchant_id"],how="left")

#%%
# browse_days
browse_days_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["time_stamp"]]).count()\
    .reset_index()[["user_id","seller_id","time_stamp"]]
browse_days_temp1 = browse_days_temp.groupby([browse_days_temp["user_id"],browse_days_temp["seller_id"]]).count().\
    reset_index()
browse_days_temp1.rename(columns={"seller_id":"merchant_id","time_stamp":"browse_days"},inplace=True)
df_train = pd.merge(df_train,browse_days_temp1,on=["user_id","merchant_id"],how="left")

#%%
# one_clicks
one_clicks_temp = user_log.groupby([user_log["user_id"],user_log["seller_id"],user_log["action_type"]]).count()\
    .reset_index()[["user_id","seller_id","action_type","item_id"]]
one_clicks_temp.rename(columns={"seller_id":"merchant_id","item_id":"times"},inplace=True)
one_clicks_temp["one_clicks"] = one_clicks_temp["action_type"] == 0
one_clicks_temp["one_clicks"] = one_clicks_temp["one_clicks"] * one_clicks_temp["times"]

#%%
# shopping_carts, purchase_times, favourite_times
one_clicks_temp["shopping_carts"] = one_clicks_temp["action_type"] == 1
one_clicks_temp["shopping_carts"] = one_clicks_temp["shopping_carts"] * one_clicks_temp["times"]

one_clicks_temp["purchase_times"] = one_clicks_temp["action_type"] == 2
one_clicks_temp["purchase_times"] = one_clicks_temp["purchase_times"] * one_clicks_temp["times"]

one_clicks_temp["favourite_times"] = one_clicks_temp["action_type"] == 3
one_clicks_temp["favourite_times"] = one_clicks_temp["favourite_times"] * one_clicks_temp["times"]

#%%
four_features = one_clicks_temp.groupby([one_clicks_temp["user_id"],one_clicks_temp["merchant_id"]]).sum().reset_index()
four_features = four_features.drop(["action_type","times"], axis=1)
df_train = pd.merge(df_train,four_features,on=["user_id","merchant_id"],how="left")

#%%
# 特征缺失值处理
#df_train.info()
#df_train.isnull().sum(axis=0)
df_train = df_train.fillna(method='ffill')
#df_train.info()
#%%
from pyspark.sql import SparkSession
sc = SparkSession.builder.master("local[*]").appName("SparkML-example.com").getOrCreate()
spark_df_train = sc.createDataFrame(df_train)
spark_df_train.show(20)

#%%
feature_columns = spark_df_train.columns[3:]
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=feature_columns,outputCol="features")
raw_data=assembler.transform(spark_df_train)
raw_data.select("features").show(truncate=False)

#%%

from pyspark.ml.feature import StandardScaler
standardscaler=StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
raw_data=standardscaler.fit(raw_data).transform(raw_data)
raw_data.select("features","Scaled_features").show(20)

#%%
dataset_size=float(raw_data.select("label").count())
numPositives=raw_data.select("label").where('label == 1').count()
per_ones=(float(numPositives)/float(dataset_size))*100
numNegatives=float(dataset_size-numPositives)
print('The number of ones are {}'.format(numPositives))
print('Percentage of ones are {}'.format(per_ones))

#%%
from pyspark.sql.functions import when
BalancingRatio= numNegatives/dataset_size
print('BalancingRatio = {}'.format(BalancingRatio))
raw_data=raw_data.withColumn("classWeights", when(raw_data.label == 1,BalancingRatio).otherwise(1-BalancingRatio))
raw_data.select("label","classWeights").show(20)
#%%

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol="label", featuresCol="Scaled_features",weightCol="classWeights",maxIter=10)
model=lr.fit(raw_data)
predict_train=model.transform(raw_data)
predict_train.select("label","prediction").show(20)
#%%
import pyspark.sql.functions as F
check = predict_train.withColumn('correct', F.when(F.col('label') == F.col('prediction'), 1).otherwise(0))
check.groupby("correct").count().show()

#%%
pd_predict_train = predict_train.toPandas()
pd_predict_train.to_csv("out/predict_train.csv", index=False)

#%%
df_test = pd.merge(df_test,user_info,on="user_id",how="left")
df_test = pd.merge(df_test,total_logs_temp,on=["user_id","merchant_id"],how="left")
df_test = pd.merge(df_test,unique_item_ids_temp1,on=["user_id","merchant_id"],how="left")
df_test = pd.merge(df_test,categories_temp1,on=["user_id","merchant_id"],how="left")
df_test = pd.merge(df_test,browse_days_temp1,on=["user_id","merchant_id"],how="left")
df_test = pd.merge(df_test,four_features,on=["user_id","merchant_id"],how="left")
df_test = df_test.fillna(method='bfill')
df_test = df_test.fillna(method='ffill')
# 缺失值向后填充

#%%
spark_df_test = sc.createDataFrame(df_test)
spark_df_test.show(20)

#%%
spark_df_test = spark_df_test.withColumnRenamed("prob", "label")
feature_columns_test = spark_df_test.columns[3:]
assembler_test = VectorAssembler(inputCols=feature_columns_test,outputCol="features")
raw_data_test = assembler_test.transform(spark_df_test)
raw_data_test.select("features").show(truncate=False)

#%%
raw_data_test = standardscaler.fit(raw_data_test).transform(raw_data_test)
raw_data_test.select("features","Scaled_features").show(20)

#%%
predict_test = model.transform(raw_data_test)
predict_test.select("probability","prediction").show(20)

#%%
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

logit_prob=udf(lambda v:float(v[1]),FloatType())
predict_test = predict_test.withColumn("prob", logit_prob('probability'))
predict_test.show(10)

#%%
final_result = predict_test.select("user_id","merchant_id","prob")
final_result.show(10)

#%%
#final_result.write.format("csv").save("out/final_result.csv")
pd_final_result = final_result.toPandas()
pd_final_result.to_csv("out/predict_test.csv",index=False)
