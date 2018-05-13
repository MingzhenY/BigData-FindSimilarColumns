#in pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.mllib.linalg import *
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Word2Vec
from pyspark.sql.utils import AnalysisException
'''
in dumbo : ./Bigdata_Load

./load.sh 25cx-4jug.json
./load.sh 27h8-t3wt.json
./load.sh 28rh-vpvr.json
./load.sh 29bv-qqsy.json
./load.sh 2fws-68t6.json
./load.sh 2q48-ip9a.json
./load.sh 2x8v-d8nh.json
./load.sh 2zbg-i8fx.json
./load.sh 32y8-s55c.json
./load.sh 33c5-b922.json

./load.sh 25aa-q86c.json
./load.sh 25th-nujf.json
./load.sh 29km-avyc.json
./load.sh 29ry-u5bf.json
./load.sh 2cmn-uidm.json
./load.sh 2xir-kwzz.json
./load.sh 2xh6-psuq.json
./load.sh 39g5-gbp3.json
./load.sh 3mrr-8h5c.json

./load.sh zkky-n5j3.json
./load.sh yu9n-iqyk.json
./load.sh yjub-udmw.json
./load.sh yhuu-4pt3.json
./load.sh yeba-ynb5.json
./load.sh xqmg-7z3j.json
./load.sh xck4-5xd5.json
./load.sh x5tk-fa54.json
./load.sh ws4c-4g69.json
'''


files = ['25aa-q86c.json','25th-nujf.json','29km-avyc.json','29ry-u5bf.json',\
    '2cmn-uidm.json','2xir-kwzz.json','2xh6-psuq.json','39g5-gbp3.json',\
    '3mrr-8h5c.json',\
    '25cx-4jug.json','27h8-t3wt.json','28rh-vpvr.json','29bv-qqsy.json',\
    '2fws-68t6.json','2q48-ip9a.json','2x8v-d8nh.json','2zbg-i8fx.json',\
    '32y8-s55c.json','33c5-b922.json',\
    'zkky-n5j3.json','yu9n-iqyk.json','yjub-udmw.json',\
    'yhuu-4pt3.json','yeba-ynb5.json','xqmg-7z3j.json','xck4-5xd5.json',\
    'x5tk-fa54.json','ws4c-4g69.json']


def columnCount(data):
    ret = 0
    for key in data:
        ret += len(data[key].schema.fields)
    return ret


def getColumnNames(data):
    ret = []
    for table_name in data:
        for column_name in data[table_name].schema.names:
            ret.append((table_name,column_name))
    return ret


def getColumnRDD(data,table_name, column_name):
    if table_name in data:
        if column_name in data[table_name].schema.names:
            return data[table_name].select(column_name).rdd\
                    .map(lambda x:x[0]).filter(lambda x: x is not None)\
                    .map(lambda x:str(x))
        else:
            return
    else:
        return


def getTableNames(data,table_name):
    if table_name in data:
        ret = []
        for column_name in data[table_name].schema.names:
            ret.append((table_name,column_name))
        return ret
    else:
        return


data = dict([(name,spark.read.json(name)) for name in files])

# (table_name, column_name)
names = getColumnNames(data)

# (table_name, column_name, rdd)
cols = [(t,c,getColumnRDD(data,t,c)) for (t,c) in names]



