#in pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.mllib.linalg import *
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Word2Vec
from pyspark.sql.utils import AnalysisException
import random
import time


def getFrequent(rdd,n):
    return (dict(rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)\
        .sortBy(lambda x:x[1]).zipWithIndex().filter(lambda x:x[1]<n)\
        .map(lambda x:x[0]).collect()),rdd.count())



def jaccard_sim(dxc,dyc):
    dx = dxc[0]
    dy = dyc[0]
    dx_count = dxc[1]
    dy_count = dyc[1]
    
    d_union = {}
    d_intersect = {}
    
    for key in dx:
        if key in d_union:
            d_union[key] += dx[key]
        else:
            d_union[key] = dx[key]
    
    for key in dy:
        if key in d_union:
            d_union[key] += dy[key]
        else:
            d_union[key] = dy[key]
    
    #print(d_union)
    for key in d_union:
        if key in dx and key in dy:
            if dx[key] < dy[key]:
                d_intersect[key] = dx[key]
            else:
                d_intersect[key] = dy[key]
    #print(d_intersect)
    for key in d_intersect:
        d_union[key] -= d_intersect[key]
    #print(d_union)
    
    count_union = 0
    count_intersect = 0
    for key in d_union:
        count_union += d_union[key]
    for key in d_intersect:
        count_intersect += d_intersect[key]
    
    try:
        return count_intersect / (dx_count + dy_count - count_intersect)
    except ZeroDivisionError:
        return 0.0
    

def FindSimilarColumns_FV(cols,min_sim = 0.7,n = 100):
    start_time = time.time()
    freq_cols = {}
    count = 0
    for key in cols:
        count += 1 
        print(count,key)
        freq_cols[key] = getFrequent(cols[key],n)
    
    keys = list(freq_cols.keys())
    L = len(keys)
    ret = {}
    for x in range(L):
        for y in range(x+1,L):
            #print(x,y,'/',L)
            sim = jaccard_sim(freq_cols[keys[x]],freq_cols[keys[y]])
            if sim > min_sim:
                ret[(keys[x],keys[y])] = 1-sim
    end_time = time.time()
    print('time = {0:.0f} s'.format(end_time - start_time))
    print('size =',len(ret))
    return ret


if __name__ == '__main__':
    pairs = FindSimilarColumns_FV(cols,0.6,100)
    
    
    
