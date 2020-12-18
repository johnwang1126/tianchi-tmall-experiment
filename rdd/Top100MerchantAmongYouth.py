import sys
from pyspark import SparkContext, SparkConf
import pandas as pd
sys.path.insert(0, '.')

if __name__ == "__main__":
    conf = SparkConf().setAppName("top_100_merchant").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 读入用户信息以及商品信息
    users = sc.textFile("in/user_info_format1.csv")
    items = sc.textFile("in/user_log_format1.csv")
    # 按条件筛选数据
    usersYouth = users.filter(lambda line: line.split(',')[1] == "1" or line.split(',')[1] == "2" or line.split(',')[1] == "3")
    itemsDoubleEleven = items.filter(lambda line: line.split(',')[5] == "1111" and line.split(",")[6] != "0")
    # 对同一用户同一商品同一行为进行去重
    UserItemAction = itemsDoubleEleven.map(
        lambda line: ((line.split(",")[0], line.split(",")[1], line.split(",")[3], line.split(",")[6]), 1))
    itemsDeduplication = UserItemAction.reduceByKey(lambda x, y: x)
    # join两个RDD
    merchantRdd = itemsDeduplication.map(lambda x: (x[0][0], x[0][2]))
    userRdd = usersYouth.map(lambda line: (line.split(',')[0], 1))
    join = merchantRdd.join(userRdd)
    # 对join后的RDD进行wordcount排序，取前100
    joinPairRdd = join.map(lambda x: (x[1][0], 1))
    joinToCountPairs = joinPairRdd.reduceByKey(lambda x, y: x + y)
    sortedMerchantCountPairs = joinToCountPairs.sortBy(lambda wordcount: wordcount[1], ascending=False).take(100)
    # 输出结果
    print("双十一最受年轻人关注TOP100商家：【添加购物⻋+购买+添加收藏夹】次数")
    result = pd.DataFrame(columns=['merchant_id', 'Number'])
    numCounter = 0
    for merchant, count in sortedMerchantCountPairs:
        numCounter += 1
        print("No.%d" % numCounter, end="  ")
        print("{} : {}".format(merchant, count),"次")
        result = result.append({'merchant_id': merchant, 'Number': count}, ignore_index=True)
    result.to_csv("out/Top100MerchantAmongYouth.csv", index=False)


