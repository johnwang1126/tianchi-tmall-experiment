import sys
from pyspark import SparkContext, SparkConf
import pandas as pd
sys.path.insert(0, '.')

if __name__ == "__main__":
    conf = SparkConf().setAppName("top_100_item").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    items = sc.textFile("in/user_log_format1.csv")
#    items = sc.textFile("in/little.csv")

    # 筛选双十一非单击商品
    itemsDoubleEleven = items.filter(lambda line: line.split(',')[5] == "1111" and line.split(",")[6] != "0")
    # 对同一用户同一商品同一行为进行去重
    UserItemAction = itemsDoubleEleven.map(lambda line: ((line.split(",")[0], line.split(",")[1], line.split(",")[6]), 1))
    itemsDeduplication = UserItemAction.reduceByKey(lambda x, y: x)
    # 对item_id进行wordcount排序,取前一百
    itemsPairRdd = itemsDeduplication.map(lambda x: (x[0][1], 1))
    itemsToCountPairs = itemsPairRdd.reduceByKey(lambda x, y: x + y)
    sortedItemsCountPairs = itemsToCountPairs.sortBy(lambda wordCount: wordCount[1], ascending=False).take(100)
    # 输出结果
    print("双十一最热门商品TOP100商品编号：【添加购物⻋+购买+添加收藏夹】次数")
    result = pd.DataFrame(columns=['item_id', 'Number'])
    numCounter = 0
    for item, count in sortedItemsCountPairs:
        numCounter += 1
        print("No.%d"%numCounter, end="  ")
        print("{} : {}".format(item, count),"次")
        result = result.append({'item_id': item, 'Number': count}, ignore_index=True)
    result.to_csv("out/Top100Item.csv", index=False)

