import math

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


def hash_str(s):
    l = len(s)
    h = 0
    for i in range(0,l):
        h = (h * 137 + ord(s[i])) % 2147483598
    return h 


def hds_v(s,k):
    l = len(s)
    if l < k :
        return []
    ans = []
    for i in range(0,l-k+1):
        ans.append(hash_str(s[i:i+k]))
    return sorted(ans)


def hds_bucket(s,k,J):
    v = hds_v(s,k)
    l = len(v)
    #only index first p symbols of v 
    #where p > (1-J)*Ls
    p = int(math.floor((1.0-J)*l)) + 1
    ret = []
    for i in range(0,p):
        ret.append((v[i],i,l-i))
    return ret


def hds_key(x,i,p,J):
    ret = []
    #1 p >= q
    for j in range(0,int(math.floor((1-J)/J*p)-i+1)):
        for q in range(int(math.ceil(J*(p+i+j))),p+1):
            ret.append((x,j,q))
    #2 p < q
    for j in range(0,int(math.floor(p/J))-i-p):
        for q in range(p+1,int(math.floor(p/J))-i-j+1):
            ret.append((x,j,q))
    return ret


def hds_helper(s,k,J):
    buckets = hds_bucket(s,4,J)
    v = hds_v(s,k)
    ret = []
    for key in buckets:
        ret.append((key,[v]))
    return ret


def hds(x,k,J):
    ''' put each hds_v into corresponding buckets
        returns a rdd with (key,value) pairs
        where key = bucket number
              value = [hds_v]
    '''
    return x.flatMap(lambda x:hds_helper(x,k,J)).reduceByKey(lambda x,y:x+y)


def jaccard_int(s1,s2):
    '''calculate the jaccard similarity of two sets of int'''
    l1 = set(s1)
    l2 = set(s2)
    u = len(l1.intersection(l2))
    v = len(l1.union(l2))
    return u * 1.0 / v


def search_str_map(s,hds_map,k,J):
    '''search string s in indexed map hds_map'''
    v = hds_v(s,k)
    bucket = hds_bucket(s,k,J)
    keys = []
    for (x,i,p) in bucket:
        keys += hds_key(x,i,p,J)
    # got the keys
    # now search these keys in hds_kv
    for key in keys:
        if key in hds_map:
            for _v in hds_map[key]:
                if(jaccard_int(v,_v) >= J - 1e6) :
                    return True
    return False 


def jaccardEx(x,y,k,J):
    '''extended jaccard distance where x and y are column of strings
    F(x,y) = COUNT(values of x where there is a similar value in y) / COUNT(x). 
    And define DIS(x,y) = 1 - min(F(x,y),F(y,x)). 
    '''
    hds_x = hds(x,k,J)
    hds_y = hds(y,k,J)
    hds_x_map = sc.broadcast(hds_x.collectAsMap())
    hds_y_map = sc.broadcast(hds_y.collectAsMap())
    px = x.filter(lambda s:search_str_map(s,hds_y_map.value,k,J)).count() * 1.0 / x.count()
    py = y.filter(lambda s:search_str_map(s,hds_x_map.value,k,J)).count() * 1.0 / y.count()
    return 1 - min(px,py)

def dis_kgram(x,y,k):
    dx = x.flatMap(lambda s:hds_v(s,k))
    dy = y.flatMap(lambda s:hds_v(s,k))
    return jaccard_bag(dx,dy)




