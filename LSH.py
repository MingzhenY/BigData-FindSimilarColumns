#in pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.mllib.linalg import *
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Word2Vec
from pyspark.sql.utils import AnalysisException
import random
import time
from dateutil import parser

# 2^64 = 18446744073709551616
# 2^32 = 4294967296

def generateSeeds(n = 16):
    random.seed()
    ret = []
    for i in range(n):
        ret.append(random.randrange(0,4294967296))
    return ret


def hash64(seed,x):
    random.seed(seed ^ x)
    return random.randrange(0,4294967296)


def hashString(msg):
    H = len(msg) * 2654289839 % 4294967296
    for ch in msg:
        H = (H * 137 + ord(ch)) % 4294967296
    return H


def getQgram(msg,q):
    ret = []
    for i in range(0,len(msg)-q+1):
        ret.append(msg[i:i+q])
    return ret


def normalizeString(msg,q):
    '''
    1. turn all letters into lower case 
    2. combine consecutive white spaces to one space
    3. remove punctuation marks
    '''
    try:
        f = float(msg)
        #dt = parser.parse(msg)
        return []
    except ValueError:
        pass
    except OverflowError:
        pass
    
    ret_msg = ''
    last_ch = None
    for ch in msg:
        if ch.isalnum():
            last_ch = ch.lower()
            ret_msg += last_ch
        else:
            # treat any other characters as space
            # combine consecutive spaces
            if last_ch != ' ':
                ret_msg += ' '
                last_ch = ' '
    
    if len(ret_msg) < q:
        return []
    else:
        return [ret_msg]


def normalizeColumn(rdd,q):
    '''
    rdd is [string]
    '''
    rdd_nstr = rdd.flatMap(lambda x:normalizeString(x,q))
    
    if rdd_nstr.count() > rdd.count() * 0.8:
        return rdd_nstr
    else:
        return


def computeMinHashSignature(sig,seeds,x):
    for i in range(len(seeds)):
        nx = hash64(seeds[i],x)
        if nx < sig[i]:
            sig[i] = nx
    return sig


def getMinHashSignature(seeds,rdd,q,normalize = True):
    if normalize:
        rdd = normalizeColumn(rdd,q)
        #print(column_name+'.rdd=',rdd)
        if rdd is None:
            return
    
    # [string] -> [qgram] -> [qgram_index]
    rdd_qgram = rdd.flatMap(lambda x: getQgram(x,q))
    rdd_qgram_index = rdd_qgram.map(lambda x:hashString(x)).distinct()
    
    # [qgram_index] -> Signature
    sig = []
    for _ in range(len(seeds)):
        sig.append(4294967296)
    for qgram_index in rdd_qgram_index.collect():
        sig = computeMinHashSignature(sig,seeds,qgram_index)
    return sig


def testTable(seeds,df,q = 4):
    column_names = df.schema.names
    signatures = {}
    for name in column_names:
        sig = getMinHashSignature(seeds,df,name,q)
        if sig is not None:
            signatures[name] = sig
    return signatures


def LSH(sigs):
    bucket = {}
    for key in sigs:
        for i in range(8):
            bucket_name = ','.join([str(x) for x in sigs[key][i*2:i*2+2]])
            if bucket_name in bucket:
                bucket[bucket_name].add(key)
            else:
                bucket[bucket_name] = set([key])
    ret = set()
    for key in bucket:
        if len(bucket[key]) >= 2:
            l = sorted(list(bucket[key]))
            n = len(l)
            for a in range(n):
                for b in range(a+1,n):
                    ret.add((l[a],l[b]))
            #ret.append(list(bucket[key]))
    return list(ret)


def LSH2(sigs):
    bucket = {}
    for key in sigs:
        for i in range(16):
            bucket_name = ','.join([str(x) for x in sigs[key][i:i+1]])
            if bucket_name in bucket:
                bucket[bucket_name].add(key)
            else:
                bucket[bucket_name] = set([key])
    ret = set()
    for key in bucket:
        if len(bucket[key]) >= 2:
            l = sorted(list(bucket[key]))
            n = len(l)
            for a in range(n):
                for b in range(a+1,n):
                    ret.add((l[a],l[b]))
            #ret.append(list(bucket[key]))
    return list(ret)



def jaccard_bag(x,y):
    """calculate jaccard distance of x and y (x and y are rdds)"""
    u = x.subtract(y).count() + y.subtract(x).count()
    v = (x.union(y).count() - u) / 2 + u
    try:
        return u * 1.0 / v
    except ZeroDivisionError:
        return 1.0



def FindSimilarColumns_LSH(cols,b = 20,r = 7,s = 0.65, q = 4,normalize = True):
    seeds = generateSeeds(b*r)
    sigs = {}
    count = 0
    for key in cols:
        count += 1
        print(count,key)
        rdd = cols[key]
        sig = getMinHashSignature(seeds,rdd,q,normalize)
        if sig is not None:
            sigs[key] = sig
    print('sig.size =',len(sigs))
    bucket = {}
    for key in sigs:
        for i in range(b):
            bucket_name = ','.join([str(x) for x in sigs[key][i*r:(i+1)*r]])
            if bucket_name in bucket:
                bucket[bucket_name].add(key)
            else:
                bucket[bucket_name] = set([key])
    print('bucket.size =',len(bucket))
    ret = {}
    ret_keys = {}
    for key in bucket:
        if len(bucket[key]) >= 2:
            l = sorted(list(bucket[key]))
            n = len(l)
            for a in range(n):
                for b in range(a+1,n):
                    if (l[a],l[b]) not in ret_keys:
                        ret_keys[(l[a],l[b])] = True
                        rdda = cols[l[a]].flatMap(lambda x: getQgram(x,q))\
                            .map(lambda x:hashString(x)).distinct()
                        rddb = cols[l[b]].flatMap(lambda x: getQgram(x,q))\
                            .map(lambda x:hashString(x)).distinct()
                        dis = jaccard_bag(rdda,rddb)
                        if dis < (1 - s):
                            ret[(l[a],l[b])] = dis
    return ret


def getNormCols(cols,q):
    ret = {}
    start_time = time.time()
    count = 0
    for key in cols:
        count += 1
        print(count,key)
            rdd = normalizeColumn(cols[key],q)
            if rdd is not None:
                ret[key] = rdd
        end_time = time.time()
        print('time = {0:.2f} s'.format(end_time - start_time))
        return ret




def timeTest(cols,n,normalize = True):
    '''
    Note: 
        'sig.size' indicates how many columns is sutable to use this method.
        If all strings are shorter than q characters, then q-gram will not work.
        In this case the column is ignored.
    
    q = 4, (b, r) = (100, 2) , s >= 0.1, DS = old (1199 columns)
    n       time        sig.size    bucket.size     pairs.size
    10      12.12s      5           451             1
    100     359.07s     38          3065            24
    500     2878.58s    210         15015           731
    1200    11498.91s   551         33215           4183
    
    
    q = 4, (b, r) = (20, 2) , s >= 0.22   DS = old (975 columns)
    n       time        sig.size    bucket.size     pairs.size
    100     73s         31          564             10
    200     294s        68          1172            87
    
    q = 6, (b, r) = (20, 7) , s >= 0.65   DS = old (975 columns)
    n       time        sig.size    bucket.size     pairs.size
    100     176s        23          420             3
    200     450s        53          997             7
    500     879s        153         2671            110
    1000    1846s       299         5050            489
    
    q = 5, (b, r) = (20, 7) , s >= 0.65,  DS = new (379 columns)
    n       time        sig.size    bucket.size     pairs.size
    379     699s        218         4204            14
    
    q = 4, (b, r) = (20, 2) , s >= 0.22,  DS = new (379 columns)
    n       time        sig.size    bucket.size     pairs.size
    379     1312        257         4125            401
    '''
    sample_cols = dict([(key,cols[key]) for key in list(cols.keys())[:n]])
    start_time = time.time()
    #pairs = FindSimilarColumns_LSH(sample_cols,20,2,0.22,4,normalize)
    pairs = FindSimilarColumns_LSH(sample_cols,20,7,0.65,5,normalize)
    end_time = time.time()
    print('time = {0:.0f} s'.format(end_time - start_time))
    print('pairs.size =',len(pairs))
    return pairs


def showKey(data,key):
    data[key[0][0]].select(key[0][1]).distinct().where(col(key[0][1]).isNotNull()).orderBy(col(key[0][1])).show()
    data[key[1][0]].select(key[1][1]).distinct().where(col(key[1][1]).isNotNull()).orderBy(col(key[1][1])).show()


def test(n):
    key = list(pairs.keys())[n]
    print(key)
    showKey(data,key)


#FindSimilarColumns_LSH(cols,20,7,0.65,5)
pairs = {(('2fws-68t6.json', 'Service HIV/AIDS'), ('2fws-68t6.json', 'Specialize HIV/AIDS')): 0.0, (('2fws-68t6.json', 'Service Elder Abuse'), ('2fws-68t6.json', 'Specialize Elder Abuse Services')): 0.25, (('27h8-t3wt.json', 'Category'), ('yu9n-iqyk.json', 'Category')): 0.0, (('2fws-68t6.json', 'Specialize Batterers'), ('2fws-68t6.json', 'Specialize Male Survivors')): 0.3333333333333333, (('2fws-68t6.json', 'Service Stalking Victim Services '), ('2fws-68t6.json', 'Specialize Services Stalking Victims ')): 0.0, (('3mrr-8h5c.json', 'DBN'), ('yu9n-iqyk.json', 'DBN')): 0.008719346049046322, (('yhuu-4pt3.json', 'Hack Up Date'), ('yhuu-4pt3.json', 'Suspension Date')): 0.1829004329004329, (('27h8-t3wt.json', 'Category'), ('3mrr-8h5c.json', 'Demographic')): 0.0, (('yhuu-4pt3.json', 'Hack Up Date'), ('yjub-udmw.json', 'Activated')): 0.34702702702702704, (('yhuu-4pt3.json', 'Certification Date'), ('yhuu-4pt3.json', 'Suspension Date')): 0.1815217391304348, (('yhuu-4pt3.json', 'Certification Date'), ('yhuu-4pt3.json', 'Hack Up Date')): 0.006493506493506494, (('yhuu-4pt3.json', 'Certification Date'), ('yjub-udmw.json', 'Activated')): 0.34276387377584333, (('2zbg-i8fx.json', 'COMPARABLE RENTAL – 1 – Building Classification'), ('2zbg-i8fx.json', 'COMPARABLE RENTAL – 2 – Building Classification')): 0.21052631578947367, (('3mrr-8h5c.json', 'Demographic'), ('yu9n-iqyk.json', 'Category')): 0.0}


#Old results
pairs = {(('2fws-68t6.json', 'Specialize Batterers'), ('2fws-68t6.json', 'Specialize Male Survivors')): 0.3076923076923077, (('xck4-5xd5.json', 'NTA'), ('yjub-udmw.json', 'NTAName')): 0.19181479950392724, (('29ry-u5bf.json', 'DBN'), ('2x8v-d8nh.json', 'DBN')): 0.34421534936998854, (('25aa-q86c.json', 'DBN'), ('yu9n-iqyk.json', 'DBN')): 0.1755361397934869, (('25aa-q86c.json', 'School Name'), ('29ry-u5bf.json', 'School Name')): 0.03399722769387904, (('25aa-q86c.json', 'Category'), ('29ry-u5bf.json', 'Category')): 0.23357664233576642, (('2x8v-d8nh.json', 'DBN'), ('xqmg-7z3j.json', 'ATS Code')): 0.06934097421203439, (('xqmg-7z3j.json', 'Location Name'), ('yeba-ynb5.json', 'LOCATION NAME')): 0.057243255497619584, (('25aa-q86c.json', 'School Name'), ('2x8v-d8nh.json', 'LOCATION NAME')): 0.2213696417238793, (('29ry-u5bf.json', 'DBN'), ('yeba-ynb5.json', 'DBN')): 0.34421534936998854, (('2x8v-d8nh.json', 'LOCATION CATEGORY'), ('yeba-ynb5.json', 'LOCATION CATEGORY')): 0.12857142857142856, (('29ry-u5bf.json', 'School Name'), ('yeba-ynb5.json', 'LOCATION NAME')): 0.23984669946230408, (('3mrr-8h5c.json', 'DBN'), ('yu9n-iqyk.json', 'DBN')): 0.007894736842105263, (('yhuu-4pt3.json', 'Certification Date'), ('yhuu-4pt3.json', 'Suspension Date')): 0.2105997210599721, (('29ry-u5bf.json', 'School Name'), ('xqmg-7z3j.json', 'Location Name')): 0.21486875330318866, (('2fws-68t6.json', 'Service Elder Abuse'), ('2fws-68t6.json', 'Specialize Elder Abuse Services')): 0.2608695652173913, (('27h8-t3wt.json', 'Category'), ('yu9n-iqyk.json', 'Category')): 0.0, (('27h8-t3wt.json', 'Category'), ('3mrr-8h5c.json', 'Demographic')): 0.0, (('25aa-q86c.json', 'School Name'), ('yeba-ynb5.json', 'LOCATION NAME')): 0.22127978766372397, (('29bv-qqsy.json', 'LOCATION NAME'), ('yeba-ynb5.json', 'LOCATION NAME')): 0.0, (('xck4-5xd5.json', 'borough'), ('yjub-udmw.json', 'BoroName')): 0.0, (('29bv-qqsy.json', 'LOCATION CATEGORY'), ('yeba-ynb5.json', 'LOCATION CATEGORY')): 0.0, (('29ry-u5bf.json', 'School Name'), ('2x8v-d8nh.json', 'LOCATION NAME')): 0.2399336536261725, (('yhuu-4pt3.json', 'Certification Date'), ('yhuu-4pt3.json', 'Hack Up Date')): 0.009002770083102494, (('2fws-68t6.json', 'Service Stalking Victim Services '), ('2fws-68t6.json', 'Specialize Services Stalking Victims ')): 0.0, (('29bv-qqsy.json', 'LOCATION NAME'), ('xqmg-7z3j.json', 'Location Name')): 0.057243255497619584, (('25aa-q86c.json', 'DBN'), ('yeba-ynb5.json', 'DBN')): 0.332378223495702, (('25aa-q86c.json', 'DBN'), ('29ry-u5bf.json', 'DBN')): 0.01970865467009426, (('2x8v-d8nh.json', 'LOCATION NAME'), ('xqmg-7z3j.json', 'Location Name')): 0.05735010767312705, (('xqmg-7z3j.json', 'ATS Code'), ('yeba-ynb5.json', 'DBN')): 0.06934097421203439, (('29bv-qqsy.json', 'LOCATION NAME'), ('29ry-u5bf.json', 'School Name')): 0.23984669946230408, (('25aa-q86c.json', 'School Name'), ('xqmg-7z3j.json', 'Location Name')): 0.2130493457724579, (('29ry-u5bf.json', 'DBN'), ('3mrr-8h5c.json', 'DBN')): 0.19081551860649248, (('29bv-qqsy.json', 'LOCATION CATEGORY'), ('2x8v-d8nh.json', 'LOCATION CATEGORY')): 0.12857142857142856, (('29bv-qqsy.json', 'DBN'), ('2x8v-d8nh.json', 'DBN')): 0.0, (('29bv-qqsy.json', 'DBN'), ('xqmg-7z3j.json', 'ATS Code')): 0.06934097421203439, (('29ry-u5bf.json', 'DBN'), ('yu9n-iqyk.json', 'DBN')): 0.19349722442505948, (('2zbg-i8fx.json', 'COMPARABLE RENTAL – 1 – Building Classification'), ('2zbg-i8fx.json', 'COMPARABLE RENTAL – 2 – Building Classification')): 0.23529411764705882, (('3mrr-8h5c.json', 'DBN'), ('xqmg-7z3j.json', 'ATS Code')): 0.34054054054054056, (('25aa-q86c.json', 'DBN'), ('xqmg-7z3j.json', 'ATS Code')): 0.2868550368550369, (('29bv-qqsy.json', 'LOCATION NAME'), ('2x8v-d8nh.json', 'LOCATION NAME')): 0.00103836169599077, (('2fws-68t6.json', 'Service HIV/AIDS'), ('2fws-68t6.json', 'Specialize HIV/AIDS')): 0.0, (('25aa-q86c.json', 'DBN'), ('29bv-qqsy.json', 'DBN')): 0.332378223495702, (('29ry-u5bf.json', 'DBN'), ('xqmg-7z3j.json', 'ATS Code')): 0.2995702885205648, (('25aa-q86c.json', 'DBN'), ('2x8v-d8nh.json', 'DBN')): 0.332378223495702, (('xqmg-7z3j.json', 'ATS Code'), ('yu9n-iqyk.json', 'DBN')): 0.3457382953181273, (('2x8v-d8nh.json', 'LOCATION NAME'), ('yeba-ynb5.json', 'LOCATION NAME')): 0.00103836169599077, (('29bv-qqsy.json', 'DBN'), ('yeba-ynb5.json', 'DBN')): 0.0, (('yhuu-4pt3.json', 'Hack Up Date'), ('yhuu-4pt3.json', 'Suspension Date')): 0.21398891966759004, (('25aa-q86c.json', 'DBN'), ('3mrr-8h5c.json', 'DBN')): 0.17287866772402855, (('25aa-q86c.json', 'School Name'), ('29bv-qqsy.json', 'LOCATION NAME')): 0.22127978766372397, (('2x8v-d8nh.json', 'DBN'), ('yeba-ynb5.json', 'DBN')): 0.0, (('29bv-qqsy.json', 'DBN'), ('29ry-u5bf.json', 'DBN')): 0.34421534936998854, (('3mrr-8h5c.json', 'Demographic'), ('yu9n-iqyk.json', 'Category')): 0.0}
