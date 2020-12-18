import sys
from pyspark import SparkContext, SparkConf
sys.path.insert(0, '.')

if __name__ == "__main__":
    conf = SparkConf().setAppName("buyer_age").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 读入用户信息以及商品信息
    users = sc.textFile("in/user_info_format1.csv")
    items = sc.textFile("in/user_log_format1.csv")
    # 按条件筛选数据
    buyerDoubleEleven = items.filter(lambda line: line.split(',')[5] == "1111" and line.split(",")[6] == "2")
    userAgeValid = users.filter(lambda line: line.split(',')[1] != "0" and line.split(',')[1] != "")
    userRdd = userAgeValid.map(lambda line: (line.split(',')[0], line.split(',')[1]))
    # 去重
    buyerRdd = buyerDoubleEleven.map(lambda line: (line.split(',')[0], -1))
    joinBuyer = userRdd.join(buyerRdd)
    buyerAge = joinBuyer.reduceByKey(lambda x, y: x).map(lambda x: (x[1][0], x[1][1]))
    buyerAgeCount = buyerAge.count()
    print("双十一有效年龄买家总人数: {}".format(buyerAgeCount))

    ageCounts = buyerAge \
        .groupByKey() \
        .mapValues(len) \
        .collect()

    for age in ageCounts:
        print("年龄范围：%s  人数：%d" % (age[0], age[1]), end="    ")
        print("百分比：{:.2%}".format(age[1]/buyerAgeCount))