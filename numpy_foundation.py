import numpy as np
import pandas as pd
import random
import matplotlib.pyplot as plt

## tips1, slice not copy for arrays and plus+ 深拷贝，浅拷贝
aList= [i for i in range(10)]
b = aList[:-1]
print(aList, b, sep='  \n  ')
b[0] = 20
print(aList, b, sep='  \n  ')
cArray = np.arange(10)
d = cArray[:-1]
print(cArray, d, sep='  \n  ')
d[0] = 20
print(cArray, d, sep='  \n  ')
d = cArray[:-1].copy()
d[1] = 30
print(cArray, d, sep='  \n  ')
# array changed by subset change, cuz the subset not copy, just index
aSeries = pd.Series(aList)
bSeries = aSeries[:-1]
print(aSeries, bSeries, sep='  \n  ')
bSeries[0] =20
print(aSeries, bSeries, sep='  \n  ')
bSeries = aSeries[:-1].copy()
bSeries[1] = 30
print(aSeries, bSeries, sep='  \n  ')
# series changed by subset change, cuz the subset not copy, just index
aData = aSeries.to_frame()
bData = aData[:-1]
print(aData, bData, sep='  \n  ')
bData.iloc[0,0] = 20
print(aData, bData, sep='  \n  ')
bData = aData[:-1].copy()
bData.iloc[1, 0] = 30
print(aData, bData, sep='  \n  ')
# DataFrame changed by subset change, cuz the subset not copy, just index


# tips2, bool --> subset 布尔索引
aData = pd.DataFrame( {
    'qty': np.random.randint(100,200, size=100)
, 'price': np.random.uniform(100.0, 200.0, size=100)
}
#, index=[0]
)
aData.loc[aData.us_price > 300, ['qty','txn_value']]
aData[aData.us_price > 300][['qty','txn_value']]
aData.loc[aData['us_price'] > 300, ['qty','txn_value']]

# tips3, series 和标量的运算
# 174 * 164.05237
aData['us_price'] = aData['price'] * 6.8
aData['txn_value'] = aData[['price', 'qty']].apply(np.prod, axis=1)
aData['liner'] = aData[['price', 'qty']].apply(lambda x: x[0] * 100 + x[1]* 1000, axis=1)
# 148 * 100 + 163 * 1000
aData['runsum_qty'] = aData[['qty']].apply(np.cumsum)
aData[['runsum_qty', 'runsum_txn_v']] = aData[['qty', 'txn_value']].apply(np.cumsum)
#aData sliced sub-DataFrame's grAnularity is series
aData['runsum_qty'] = aData['qty'].apply(np.cumsum)
#aDtaa sliced sub-Series's granularity is cell(field)


# tips4, 关于python列表(list)切片[start:stop:step]的理解
charList = list(map(chr, range(ord('a'), ord('e')+1)))
charList[0:len(charList):2] # step controls direction
charList[::-1] #means reverse the charList


# tips5, np 运算

#partI, structure method
np.linspace(start=0.0, stop=10.0, num=1000) #linespace is evenly slicd by num
np.arange(0, 10, step = 0.1) #arange is gradually increased by step
np.array([[1,2], [3,4]])

np.zeros(shape= (2,2), )
np.ones(shape= (2,2), )
a = np.random.randint(1,10, size=(3,3))
np.ones_like(a) #ones_like, zero_like pass a setup  array and convert it into ones/zeros
np.zeros_like(a)
np.empty(shape=(3,3)) #empty array created shape and pass the residual data in ram

np.identity(3)
np.eye(3) #Return a 2-D array with ones on the diagonal and zeros elsewhere.
# when M=N then eye(3) == identity(3)
np.eye(3,M=4, k=1)  # upper the index of diagonal ones
np.eye(3,M=4, k=-1) # lower the index of diagnol ones

np.full( shape= (2, 2), fill_value = np.inf)
np.full((2, 2), np.nan)
# inf is infinite, nan is not a number
np.nan is np.nan #True
np.nan == np.nan # False !!!!


#partII, calc method

#set1, pie
x1 = np.arange(1,10)
x2 = np.arange(2,11)
np.hypot(x1, x2) #= sqrt(x1**2 + x2**2)
np.pi #3.14
rad = np.arange(12.)*np.pi/6
np.degrees(rad)
deg = np.arange(0,360,30)
np.radians(deg)

#set2, round
np.round([-1.7, -1.3, 1.3, 1.7], decimals=0)
np.rint([-1.7, -1.3, 1.3, 1.7])
np.trunc([-1.7, -1.3, 1.3, 1.7]) # array([-1., -1.,  1.,  1.])
np.floor([-1.7, -1.3, 1.3, 1.7]) # array([-2., -2.,  1.,  1.])
#no matter posi or nega,floor func is one direction, to the negative <--
np.ceil([-1.7, -1.3, 1.3, 1.7])  # array([-1., -1.,  2.,  2.])
#no matter posi or nega,floor func is one direction, to the positve -->
np.fix([-1.7, -1.3, 1.3, 1.7])  # array([-1., -1.,  1.,  1.])
# round towards zero

#set3, agg func
np.sum([1,2,3,4])
np.sum([1,2,3,np.nan]) #= nan
np.nansum([1,2,3,np.nan]) #=6
np.cumsum([1,2,3,np.nan])
np.nancumsum([1,2,3,np.nan]) #treat nan as 0 in sum
np.prod([1,2,3,4])
np.prod([1,2,3,np.nan]) #= nan
np.nanprod([1,2,3,np.nan]) #=6
np.cumprod([1,2,3,np.nan])
np.nancumprod([1,2,3,np.nan]) #treat nan as 1 in prod

#set4, diff func
np.diff([[1, 3, 6, 10], [0, 5, 6, 8]])
np.diff([[1, 3, 6, 10], [0, 5, 6, 8]], axis=0)
aData = pd.DataFrame([[1, 3, 6, 10], [0, 5, 6, 8]], columns=['a', 'b', 'c', 'd'])
aData['E'] = aData[['b', 'c', 'd']].apply(np.diff, axis=1)
aData['EE'] = aData[['c', 'd']].apply(lambda p: np.diff(p)[0], axis=1)


#set5, maximum etc,  二元函数

random.seed(112335)
tada = pd.DataFrame(
    {   'A': [random.randint(0,10) for _ in range(10)]
      , 'B': [random.uniform(0.0,10.0) for _ in range(10)]
    }
)
# 5/9/4/4/0/5/5/4/1/9
# modf: Return the fractional and integral parts of an array, element-wise.
tada['frac'], tada['integ'] = tada['B'].apply(np.modf)
# maximum pass two args
tada['C'] = np.maximum(tada['A'],  tada['B'])
tada['D'] = tada[['A', 'B']].apply(np.max, axis=1)
# tada[['A', 'B']].apply(max, axis=1)
np.maximum([2, 3, 4], [1, 5, 2]) #-->: array([2, 5, 4])
# quotient, remainder func
3.5 == 2* (3.5//2) + (3.5 % 2)
quotient, remainder = np.divmod(3.5, 2)
# narrow the values within range(min, max)
tada['D'] = tada['D'].apply(lambda q: np.clip(q, 5,6.3))
# nan_to_num
tada.loc[9, 'D'] = np.nan
tada.loc[7, 'D'] = np.inf
tada['D'].apply(np.nan_to_num)
# negative
tada['NEG'] = tada['A'].apply(np.negative)
# 插值, xp, fp 形成一个回归线，x 通过 interp 函数映射到回归线上， 返回Y值
x = [0, 1, 1.5, 2.72, 3.14]
xp = [1, 2, 3]
fp = [3, 2, 0]
y = np.interp(x, xp, fp)  # array([ 3. ,  3. ,  2.5 ,  0.56,  0. ])
plt.plot(xp, fp, '-o')
plt.plot(x, y, 'x')
plt.show()


# set6, 矩阵运算，shape 需要至少一个维度MATCH
x1 = np.arange(9.0).reshape((3, 3))
x2 = np.arange(3.0).reshape((3,1))
np.subtract(x1, x2)
np.add(x1, x2)
np.multiply(x1,x2)
ones = np.ones(6)
twos = np.full(6, fill_value=2)
twos[3:] = [3] * 3
#ones.shape
np.add(tada, ones)
tada.apply(lambda q: np.add(q,ones), axis=1)
np.subtract(tada, ones)
np.divide(tada, twos)
np.power(tada, twos)   #tada**twos --5**2
# broadcast need match the shape, ones shape (6,1), tada shapes(6,1)

