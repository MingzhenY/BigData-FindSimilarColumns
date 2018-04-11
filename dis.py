def jaccard(x,y):
    """x and y are rdds with only one element per row
    lx , ly = gen.generateTestData_1(10000,0.2)
    rddx = spark.sparkContext.parallelize(lx)
    rddy = spark.sparkContext.parallelize(ly)
    jaccard(rddx,rddy) = 0.8
    """
    u = x.union(y).subtract(x.subtract(y)).subtract(y.subtract(x)).count()
    v = x.union(y).count()
    return 1.0 - u * 1.0 / v