### 第四部分：用户消费行为分析
abORN = """- 用户分层
    - 分析得出每个用户的总购买量和总消费金额and最近一次消费的时间的表格rfm
    - RFM模型设计
        - R表示客户最近一次交易时间的间隔。
            - /np.timedelta64(1,'D')：去除days
        - F表示客户购买商品的总数量,F值越大，表示客户交易越频繁，反之则表示客户交易不够活跃。
        - M表示客户交易的金额。M值越大，表示客户价值越高，反之则表示客户价值越低。
        - 将R，F，M作用到rfm表中
    - 根据价值分层，将用户分为：
        - 重要价值客户
        - 重要保持客户
        - 重要挽留客户
        - 重要发展客户
        - 一般价值客户
        - 一般保持客户
        - 一般挽留客户
        - 一般发展客户
            - 使用已有的分层模型即可rfm_func
"""

import pandas as pd
import datetime, random, os
import numpy as np
from luffy._luffy_func import filePath, plot_line_chart
#   user_id:用户ID
#   order_dt:购买日期
#   order_product:购买产品的数量
#   order_amount:购买金额
aData = pd.read_csv(os.path.join(filePath, 'mock_data.csv'))
aData.drop(aData.columns[0], axis=1, inplace=True)
aData['order_dt'] = aData['order_dt'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
aData['order_month'] = aData['order_month'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

# aData.dtypes
"""
- 用户第一次消费的月份分布，和人数统计
    - 绘制线形图
- 用户最后一次消费的时间分布，和人数统计
    - 绘制线形图
"""

#aData = aData[[col for col in aData.columns if col != aData.columns[0]]]
df_1 = aData.groupby('user_id')['order_month'].first().to_frame().reset_index(drop=False)
df_1 = df_1.rename(columns={'order_month': 'first_month'})
#...
df_2 = df_1.groupby('first_month')['user_id'].count().to_frame().reset_index(drop=False).rename(columns={'user_id': 'count_user'})
daDict = {}
daDict['x'] = df_2['first_month']
daDict['count_user'] = df_2['count_user']
daDict['xlabel'] = 'first_month'
daDict['ylabel'] = 'count_new_user'
daDict['type'] = 'plot'
daDict.keys()
plot_line_chart(daDict)


df_3 = aData.groupby('user_id')['order_month'].last().to_frame().reset_index(drop=False).rename(columns={'order_month': 'last_month'})
df_4 = df_3.groupby('last_month')['user_id'].count().to_frame().reset_index(drop=False).rename(columns={'user_id': 'count_user'})
taDict = {}
taDict['x'] = df_4['last_month']
taDict['count_user'] = df_4['count_user']
taDict['xlabel'] = 'last_month'
taDict['ylabel'] = 'count_new_user'
taDict['type'] = 'plot'
plot_line_chart(taDict)

"""
- 新老客户的占比
    - 消费一次为新用户
    - 消费多次为老用户
        - 分析出每一个用户的第一个消费和最后一次消费的时间
            - agg(['func1','func2']):对分组后的结果进行指定聚合
        - 分析出新老客户的'消费'比例
"""

## todo:: this method too slow-- check
for index, row in aData.iterrows():
    usr_id = row['user_id']
    subset = aData.loc[aData['user_id'] == usr_id,:]
    txns_count = len(subset)
    print(usr_id)
    if txns_count > 1:
        new_customer = False
        orderList = subset['order_dt'].tolist()
        orderList.sort()
        first_day = orderList[0]
        last_day = orderList[len(orderList)-1]
        duration = last_day -first_day
    else:
        new_customer = True
        duration = None
    aData.loc[index, 'new_customer'] = new_customer
    aData.loc[index, 'shopping_span'] = duration
    aData.loc[index, 'txns_count'] = txns_count
pivot_1 = aData.pivot_table(index='new_customer', values='order_price', aggfunc=sum)

## method II, processing to the whole DF

df_a = aData.groupby('user_id')['transaction_id'].count().to_frame().rename(columns={'transaction_id': 'txns_cnt'}).reset_index(drop=False)
df_f = aData.groupby('user_id')['order_dt'].first().to_frame().rename(columns={'order_dt': 'first_date'}).reset_index(drop=False)
df_l = aData.groupby('user_id')['order_dt'].last().to_frame().rename(columns={'order_dt': 'last_date'}).reset_index(drop=False)

aData = aData.merge(df_a, how='right', on='user_id').merge(df_f, how='right', on='user_id').merge(df_l, how='right', on='user_id')
aData['is_return'] = aData['txns_cnt'].apply(lambda x: True if x>1 else False)
aData['shopping_span'] = None
aData.loc[aData['is_return'] == True, 'shopping_span'] = aData['last_date']- aData['first_date']
aData['shopping_span'] = aData['shopping_span'].apply(lambda x: float(x)/(3600*24*1000000000.0))
# aData.dtypes

pivot_2 = aData.pivot_table(index='is_return', values='order_price', aggfunc=sum)

# index 48-- 1000000000.0
# datetime.datetime(1997,9,10) - datetime.datetime(1997,1,1)
# aData.loc[48,'shopping_span']/ (252* 3600 * 24)
## todo:: 更加深度理解 LAMBDA， 颗粒度，
## todo:: 掉落逻辑区间？？？-- loop + dict
## unique(), value_count(),
rfm = aData.pivot_table(index='user_id',aggfunc={'product_qty':'sum','order_price':'sum','order_dt':"max"})
rfm[['order_price', 'product_qty']].apply(lambda x: x-x.mean())
# apply(axis=0, default means columns

rfm['label'] = rfm[['order_price', 'product_qty']].apply(lambda x: ','.join(list(map(lambda y :'niu' if y >= 30 else 'laz', x)))
                                                         , axis=1)
# first lambda x: series,  2nd lambda y: element in list
# apply(axis=0, default means columns, axis=0 means row)