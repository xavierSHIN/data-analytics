### 第五部分：用户的生命周期
vStr = """
- 将用户划分为活跃用户和其他用户
    - 统计每个用户每个月的消费次数
    - 统计每个用户每个月是否消费，消费记录为1否则记录为0
        - 知识点：DataFrame的apply和applymap的区别
            - applymap:返回df
            - 将函数做用于DataFrame中的所有元素(elements)
            - apply:返回Series
            - apply()将一个函数作用于DataFrame中的每个行或者列
    - 将用户按照每一个月份分成：
        - unreg:观望用户（前两月没买，第三个月才第一次买,则用户前两个月为观望用户）
        - unactive:首月购买后，后序月份没有购买则在没有购买的月份中该用户的为非活跃用户
        - new:当前月就进行首次购买的用户在当前月为新用户
        - active:连续月份购买的用户在这些月中为活跃用户
        - return:购买之后间隔n月再次购买的第一个月份为该月份的回头客"""
################################---------------------#############################------------------##########

import pandas as pd
import datetime, random, os, re
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from luffy._luffy_func import filePath
from luffy.pandas_seaborn_func import plot_line_chart, heatmap
#   user_id:用户ID
#   order_dt:购买日期
#   order_product:购买产品的数量
#   order_amount:购买金额

## todo:: value_counts() ??
## todo:: double map& map ??
# link: https://www.zhihu.com/people/weng-hai-yi-90/posts

def brackets(laz):
    thatStr = ''
    condi = { 'unreg':  '^nny.*',
'unactive':  '^yn{17}$',
'new':  '^n{17}y$',
'active':  '.*yy.*',
'return':  '.*yn+y.*',
          }
    for key, value in condi.items():
        if re.fullmatch(value, laz):
            thatStr = key
    return thatStr

def active_status(d_list):
    status = []  # 某个用户每一个月的活跃度
    for i in range(18):
        # 若本月没有消费
        if d_list[i] == 'no':
            if len(status) > 0:
                if status[i - 1] == 'unreg':
                    status.append('unreg')
                else:
                    status.append('unactive')
            else:
                status.append('unreg')
        # 若本月消费
        else:
            if len(status) == 0:
                status.append('new')
            else:
                if status[i - 1] == 'unactive':
                    status.append('return')
                elif status[i - 1] == 'unreg':
                    status.append('new')
                else:
                    status.append('active')
    return status

###################-------- part I, data cleaning --------##########################
aData = pd.read_csv(os.path.join(filePath, 'mock_data.csv'))
aData.drop(aData.columns[0], axis=1, inplace=True)
aData['order_dt'] = aData['order_dt'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
aData['order_month'] = aData['order_month'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
#aData['user_id'] = aData['user_id'].astype(str)
aData['transaction_id'] = aData['transaction_id'].astype(str)
aData.dtypes


##################-------- part II, data modeling, craete spine --------##########################
## aka. CROSS JOIN == outer join with common key
dateIndex = pd.date_range(start=min(aData['order_dt']), end=max(aData['order_dt']), freq='M')
#dateIndex = [item.replace(day=1) for item in dateIndex]* len(aData['user_id'].unique())
#userIndex = [ [user]* len(dateIndex) for user in aData['user_id'].unique()]
monthIndex = [item.replace(day=1) for item in dateIndex]
spine = pd.DataFrame({'order_month': monthIndex, 'key':'0'})
#len(spine)
users = pd.DataFrame({'user_id': aData['user_id'].unique(), 'key': '0'})
backbones= users.merge(spine, how='outer', on=['key'])[['user_id', 'order_month']]
cycleD = backbones.merge(aData, how='left', on=['user_id', 'order_month']).reset_index(drop=True)
cycleD = cycleD.loc[cycleD['user_id'].isin([i for i in range(1000)]), :].reset_index(drop=True)


###################-------- part III, apply life cycle logic --------##########################
# link: https://zhuanlan.zhihu.com/p/101284491
cycleD['month_cnt'] = cycleD.groupby(by= ['user_id', 'order_month'], as_index=False)['transaction_id'].transform('count')
cycleD['month_shopped'] = cycleD['month_cnt'].apply(lambda x: 'ys' if x >0 else 'no')

pattern_dict = cycleD.groupby('user_id')['month_shopped'].apply(lambda y: ','.join(y).split(',')).to_dict()
#pattern_dict = {k: active_status(v) for k, v in pattern_dict.items()}
#pattern_dict  = {1: pattern_dict[1], 2: pattern_dict[2]}
detailDict = {key: dict(zip(monthIndex, active_status(value))) for key, value in pattern_dict.items()}
# detailDict[1]

for index, row in cycleD.iterrows():
    user_id = row['user_id']
    month = row['order_month']
    cast_value = detailDict[user_id][month]
    print(user_id, month, cast_value)
    cycleD.loc[index, 'cast_value'] = cast_value

sample = cycleD[['user_id', 'cast_value', 'order_month']].drop_duplicates(keep='first')
df_1 = sample.pivot_table(index='cast_value', columns='order_month', values='user_id', aggfunc='count')
df_1.columns = [str(col).split(' ')[0] for col in df_1.columns]
df_2 = sample.pivot_table(index='order_month', columns='cast_value', values='user_id', aggfunc='count')
df_2.index = [str(col).split(' ')[0] for col in df_2.index]
df_2 =  df_2.fillna(value=0)
df_1 =  df_1.fillna(value=0)

#################-------- part IV, heat map --------##########################

x_cast = df_2.columns.tolist()
order_mons = df_2.index.tolist()
ax_title = "customer life-cycle (in user cnt)"
cbar_label = 'txns cnt per month'
style_cmap = ['RdBu_r', 'viridis', "YlGn"]



#heatmap(df_2, x_cast, order_mons, style_cmap[0], ax_title, cbar_label)
heatmap(df_1, order_mons, x_cast, style_cmap[0], ax_title, cbar_label)
