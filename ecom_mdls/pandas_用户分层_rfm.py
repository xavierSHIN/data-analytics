################################---------------------#############################------------------##########
"RFM"
# 第四部分：用户分层
# key tech parts: np.select, lambda&map, pivot_tbl
haStr = """
- 用户分层
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
################################---------------------#############################------------------##########
import pandas as pd
import datetime, random, os
import numpy as np
from luffy._luffy_func import filePath
from luffy.pandas_seaborn_func import plot_line_chart, heatmap
import seaborn as sns

#   user_id:用户ID
#   order_dt:购买日期
#   order_product:购买产品的数量
#   order_amount:购买金额
#############------------------- part I, data processing ----------------------###########
aData = pd.read_csv(os.path.join(filePath, 'mock_data.csv'))
aData.drop(aData.columns[0], axis=1, inplace=True)
aData['order_dt'] = aData['order_dt'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
aData['order_month'] = aData['order_month'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
#aData['user_id'] = aData['user_id'].astype(str)
rfm = aData.groupby(by='user_id', as_index=False).agg({'order_price':'sum', 'transaction_id':'count', 'order_dt':'last'})
rfm = rfm.rename(columns={'transaction_id':'txns_cnt', 'order_price':'sum_price', 'order_dt':'last_date'})
#rfm['sum_price'].describe()
sns.set_style('darkgrid')
sns.boxplot(x='sum_price',  data=rfm.iloc[:1000,:])
sns.histplot(x='sum_price',  data=rfm.iloc[:1000,:])
rfm.iloc[:1000,:].boxplot(column='sum_price')
rfm.iloc[:1000,:]['sum_price'].quantile(.75)
#hahah = pd.DataFrame([i for i in range(1, 101)], columns=['xixi'])
#hahah.quantile(.25)
# plt.show()
std = rfm['sum_price'].std()
mean = rfm.iloc[:1000,:]['sum_price'].mean()
#len(rfm)
rfm = rfm.loc[rfm['sum_price'] <= 2* std, :]
#rfm.dtypes
rfm['span'] = datetime.datetime(1998,7,1) - rfm['last_date']
rfm['span'] = rfm['span'].dt.days.astype('int')
rfm['user_id'] = rfm['user_id'].astype(str)


###############------------------- PART II, categorized R,F,M, main body ----------------------###########

brackets = [ (rfm['txns_cnt'] >0) & (rfm['txns_cnt'] <=5)
             , (rfm['txns_cnt'] >5) & (rfm['txns_cnt'] <=10)
             , (rfm['txns_cnt'] > 10) & (rfm['txns_cnt'] <=15)
             , (rfm['txns_cnt'] > 15)
             ]
brackets_2 = [ (rfm['sum_price'] >0) & (rfm['sum_price'] <=50)
             , (rfm['sum_price'] >50) & (rfm['sum_price'] <=100)
             , (rfm['sum_price'] > 100) & (rfm['sum_price'] <=150)
             , (rfm['sum_price'] > 150)
             ]
brackets_3 = [ (rfm['span'] >0) & (rfm['span'] <=90)
             , (rfm['span'] >90) & (rfm['span'] <=180)
             , (rfm['span'] > 180) & (rfm['span'] <=270)
             , (rfm['span'] > 270)
             ]
rfm['R'] = np.select(condlist=brackets_3, choicelist=['4','3','2','1'])
rfm['F'] = np.select(condlist=brackets, choicelist=['1','2','3','4'])
rfm['M'] = np.select(condlist=brackets_2, choicelist=['1','2','3','4'])
## rfm[[]] is a dataframe
rfm['cate'] = rfm[['R','F','M']].apply(lambda x: ''.join(
    list(
    map(lambda y: 'no' if y in ['1','2'] else 'ys', x)
                                                        )
                                                         )
                                       , axis=1)

bins = [
rfm['cate'] == 'ysysys',
rfm['cate'] == 'noysys',
rfm['cate'] == 'nonoys',
rfm['cate'] == 'ysnoys',
rfm['cate'] == 'ysysno' ,
rfm['cate'] == 'noysno' ,
rfm['cate'] == 'nonono' ,
rfm['cate'] == 'ysnono'
]
choices = [  '重要价值客户'
        , "重要保持客户"
        , "重要挽留客户"
        , "重要发展客户"
        , "一般价值客户"
        , "一般保持客户"
        , "一般挽留客户"
        , '一般发展客户' ]
rfm['label'] = np.select(bins, choices)
rfm['label'].unique()

###############-------------------  part III, plotting & BI ----------------------###########

#df_1 = rfm.pivot_table(index='label', values= 'user_id', aggfunc='count')
df_2 = rfm.pivot_table(index='label', aggfunc={'user_id':'count', 'sum_price': 'sum'})
rfm.pivot_table(index='label', aggfunc='mean')
# todo:: pie chart -- cnt%, total$% -- check
# rfm['R'].describe()
daDict = {
    'x': df_2['sum_price']
    , 'type': 'pie'
    , 'labels': df_2.index.tolist()
    , 'xlabel': 'xixixi'
    , 'ylabel': 'hahahah'
}
plot_line_chart(daDict)
# rfm['R'].describe()
daDict = {
    'x': rfm['R']
    , 'type': 'hist'
    , 'nbins': 16
    , 'xlabel': 'txns_cnt'
    , 'ylabel': 'txns_cnt'
}
plot_line_chart(daDict)

