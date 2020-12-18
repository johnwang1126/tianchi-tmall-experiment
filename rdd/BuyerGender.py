import sys
from pyspark import SparkContext, SparkConf
sys.path.insert(0, '.')

if __name__ == "__main__":
    conf = SparkConf().setAppName("buyer_gendere").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 读入用户信息以及商品信息
    users = sc.textFile("in/user_info_format1.csv")
    items = sc.textFile("in/user_log_format1.csv")
    # 按条件筛选数据
    buyerDoubleEleven = items.filter(lambda line: line.split(',')[5] == "1111" and line.split(",")[6] == "2")
    userMaleFemale = users.filter(lambda line: line.split(',')[2] == "0" or line.split(',')[2] == "1")
    userRdd = userMaleFemale.map(lambda line: (line.split(',')[0], line.split(',')[2]))
    # 去重
    buyerRdd = buyerDoubleEleven.map(lambda line: (line.split(',')[0], -1))
    joinBuyer = userRdd.join(buyerRdd)
    buyerGender = joinBuyer.reduceByKey(lambda x, y: x).map(lambda x: (x[1][0], x[1][1]))

    # 统计男女买家人数
    genderCounts = buyerGender \
        .groupByKey() \
        .mapValues(len) \
        .collect()
    maleCount = 0
    femaleCount = 0
    for gender in genderCounts:
        if gender[0] == "1":
            maleCount = gender[1]
        else:
            femaleCount = gender[1]
    print("双十一男性买家 : 双十一女性买家  =  %d : %d"%(maleCount,femaleCount))
    print("男女比例为：%f" % (maleCount/femaleCount))

