def jaccard_bag(x,y):
    """x and y are rdds with only one element per row
    lx , ly = gen.generateTestData_1(10000,0.2)
    rddx = spark.sparkContext.parallelize(lx)
    rddy = spark.sparkContext.parallelize(ly)
    jaccard(rddx,rddy) = 0.8
    """
    u = x.subtract(y).count()+y.subtract(x).count()
    v = x.union(y).count()
    return 1.0 - u * 1.0 / v

def jaccard_set(x,y):
    '''eliminate duplicate before calculating jaccard distance'''
    dx = x.distinct()
    dy = y.distinct()
    u = dx.subtract(dy).count()+dy.subtract(dx).count()
    v = dx.union(dy).count()
    return 1.0 - u * 1.0 / v

