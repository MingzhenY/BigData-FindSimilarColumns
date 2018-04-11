def test_jaccard_1():
    lx = repeatNList(10,generateList(0,10))
    ly = repeatNList(2,generateList(5,15)) + repeatNList(10,generateList(7,15))
    rddx = spark.sparkContext.parallelize(lx)
    rddy = spark.sparkContext.parallelize(ly)
    dis_set = jaccard_set(rddx,rddy)
    dis_bag = jaccard_bag(rddx,rddy)
    return 'dis_set = '+str(dis_set)+', dis_bag = '+str(dis_bag)

