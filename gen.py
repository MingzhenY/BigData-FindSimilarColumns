import random

def generateList(l,r):
    ans = []
    for i in range(l,r) :
        ans.append(i)
    return ans

def generateRandomList(n,l,r):
    ans = []
    for i in range(0,n):
        ans.append(random.randint(l,r))
    return ans
    
def generateTestData_1(n,r):
    """generate two list of n records where n*r of them are the same"""
    if r <= 0.0 : r = 0.0
    if r >= 1.0 : r = 1.0
    pxy = int(n * r)
    lx = generateRandomList(n - pxy,0,30000)
    ly = generateRandomList(n - pxy,70000,100000)
    for i in range(0,pxy):
        v = random.randint(30000,70000)
        lx.append(v)
        ly.append(v)
    return (lx,ly)