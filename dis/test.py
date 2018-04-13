def test_jaccard_1():
    lx = repeatNList(1,generateList(0,10)) + repeatNList(5,generateList(2,10))
    ly = repeatNList(1,generateList(2,12)) + repeatNList(5,generateList(2,10))
    rddx = sc.parallelize(lx)
    rddy = sc.parallelize(ly)
    dis_set = jaccard_set(rddx,rddy)
    dis_bag = jaccard_bag(rddx,rddy)
    return 'dis_set = '+str(dis_set)+', dis_bag = '+str(dis_bag)


def test_jaccard_ex_1():
    rddx = sc.parallelize(['this is a string', 'this might not be a string', 'this si a strnig', 'this might be a string', 'this is not a string'])
    rddy = sc.parallelize(['this si a strnig', 'tihs might ont be a srting', 'htis is a strngi', 'thsi imght eb a srting', 'htis si ont a tsrign'])
    dis_ex = jaccard_ex(rddx,rddy,4,0.9)
    return dis_ex

