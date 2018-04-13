import random

def generateList(l,r):
    """generate a list of [l,r)"""
    ans = []
    for i in range(l,r) :
        ans.append(i)
    return ans


def generateRandomList(n,l,r):
    '''generate a n-element list where each element is in range [l,r)'''
    ans = []
    for i in range(0,n):
        ans.append(random.randint(l,r))
    return ans


def generateTestData_1(n,r):
    """generate two lists of n elements where n*r of them are the same"""
    if r <= 0.0 : r = 0.0
    if r >= 1.0 : r = 1.0
    pxy = int(n * r)
    lx = generateRandomList(n - pxy,0,300000000)
    ly = generateRandomList(n - pxy,700000000,1000000000)
    for i in range(0,pxy):
        v = random.randint(300000000,700000000)
        lx.append(v)
        ly.append(v)
    return (lx,ly)


def repeatNList(n,l):
    '''repeat list l n times'''
    ans = []
    for i in range(0,n):
        ans+=l
    return ans 


def generateRandomString(L):
    s = ''
    for i in range(0,L):
        r = random.randint(32,126)
        s += chr(r)
    return s


def generateRandomStringList(n,L):
    ret = []
    for i in range(0,n):
        ret.append(generateRandomString(L))
    return ret


def mutateString(s,k):
    '''switch two adjacent chars in s at a time, for k times'''
    l = len(s)
    for i in range(0,k):
        r = random.randint(0,l-2)
        ns = s[0:r]+s[r+1]+s[r]+s[r+2:]
        s = ns
    return s


def mutateStringList(ls,k):
    ret = []
    l = len(ls)
    for s in ls:
        ret.append(mutateString(s,k))
    return (ls,ret)

def generateTest_jaccardEx(n,L,k,sim):
    '''
    n string of length L
    where n*sim are mutated k times
          and n - n*sim are completely different
    '''
    nsim = int(n*sim)
    if nsim < 0 :
        nsim = 0 
    if nsim > n :
        nsim = n
    ls,rs = mutateStringList(generateRandomStringList(nsim,L),k)
    ls += generateRandomStringList(n-nsim,L)
    rs += generateRandomStringList(n-nsim,L)
    return (ls,rs)

