from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import *
from pyspark.ml.clustering import BisectingKMeans, BisectingKMeansModel, BisectingKMeansSummary
#from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy as np
from math import sqrt
from operator import add

df = spark.read.parquet("regex_table.parquet")
df1 = df.rdd.map(lambda x: (x[0],x[1],Vectors.dense(x[2])))
df1 = df1.toDF().withColumnRenamed('_1','table').withColumnRenamed('_2','colunm').withColumnRenamed('_3','features')
#df = spark.createDataFrame([["a", "a1", Vectors.dense([0.5,0.5,0.0,0.0])],\
#["a", "a2", Vectors.dense([0.1,0.2,0.3,0.4])],\
#["a", "a3", Vectors.dense([0.2,0.1,0.3,0.4])],\
#["b", "b1", Vectors.dense([0.3,0.1,0.2,0.4])],\
#["b", "b2", Vectors.dense([0.4,0.1,0.2,0.3])],\
#["b", "b3", Vectors.dense([0.5,0.5,0.0,0.0])]],\
#["table", "column", "features"])

#vso = df.rdd.map(lambda x:np.array((x[0],x[1]),x[2]))
transformed.select()sort($'prediction'.asc).show()

def model_list():
    clist = []
    df2 = df1.select('features')
    df2.cache
    df1.cache
    for i in range(2,20):
        kmeans = BisectingKMeans(k=i, minDivisibleClusterSize=1.0)
        model = kmeans.fit(df2)
        WSSSE = model.computeCost(df1)
        #print("Within Set Sum of Squared Error, k = " + str(i) + ": " +str(WSSSE))
        clist.append({i: WSSSE, 'model': model})
    df1.unpersist
    df2.unpersist
    return clist

def best_k(mlist, radius):
    m = mlist
    rlist = []
    for i in range(0,18):
        n = m[i]['model'].summary.predictions
        p = m[i]['model'].clusterCenters()
        rdd = n.rdd.map(lambda x: (x[0], Vectors.dense(p[x[1]])))
        rdd = rdd.map(lambda x: sqrt(x[0].squared_distance(x[1])))
        rlist.append({'k': i+2, 'max_r': rdd.max()})
    print(rlist)

def prediction(mlist,k):
    transformed = mlist[k-2]['model'].transform(df1)
    transformed.show(523,False)

#model = KMeans(k=3, seed=1)
#model = model.fit(df.select('features'))
#transformed = model.transform(df)
#transformed.show()
mlist = model_list()
mlist
best_k(mlist,0.48)
for i in range(2,10):
    prediction(mlist,i)

