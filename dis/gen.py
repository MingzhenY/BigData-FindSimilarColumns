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


