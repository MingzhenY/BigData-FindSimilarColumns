def jaccard_bag(x,y):
    """calculate jaccard distance of x and y (x and y are rdds)"""
    u = x.subtract(y).count() + y.subtract(x).count()
    v = x.union(y).count()
    return u * 1.0 / v

def jaccard_set(x,y):
    '''eliminate duplicate before calculating jaccard distance of x and y'''
    dx = x.distinct()
    dy = y.distinct()
    u = dx.intersection(dy).count()
    v = dx.union(dy).distinct().count()
    return 1.0 - u * 1.0 / v


