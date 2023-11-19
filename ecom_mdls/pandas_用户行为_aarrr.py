# 重点是sql 处理和 plot
# create mock dataset, user_id, session_id, view_date
# 留存率正确实现 in python

import pandas as pd
import datetime, random, os, re
import numpy as np
import random
import matplotlib.pyplot as plt
from collections import Counter
import seaborn as sns
from luffy._luffy_func import filePath
from luffy._luffy_func import Cumulative, generate_session_data
from luffy.pandas_seaborn_func import heatmap, horizontal_bar_chart


#   user_id:用户ID
#   view_dt:购买日期
#   seesion_id
# np.arange(100)
#################-------- part I, create dataset --------##########################

bData = generate_session_data()
sample = bData.loc[bData['user_id'] == '13121', :].sort_values(by='view_date')
#Counter(bData['user_id'])


#################-------- part II, metrics calc --------##########################
bData.sort_values(by=['user_id','view_date'], inplace=True)
bData['register_date'] = bData.groupby('user_id')['view_date'].transform('first')

#bData.drop(['nxt day retention'], axis=1, inplace=True)
bData['days_after'] = bData['view_date'] - bData['register_date']
bData['days_after'] = bData['days_after'].dt.days
df_1 = bData.pivot_table(index='days_after', columns='register_date', values='user_id', aggfunc='nunique')

bData['user_id_1'] = bData['user_id']
df_2 = bData.groupby('register_date').agg({'user_id': lambda x: set(','.join(x).split(','))
                                           , 'user_id_1': 'nunique'})
df_1=df_1.fillna(value=0)

#################-------- part III, visual retention rate--------##########################

for col in df_1.columns:
    print(col)
    df_1[f'{col}_%'] = df_1[[col]].apply(lambda y: y/y.max())
# df_1.drop(['2023-03-01%'], axis=1, inplace=True)
#100/300

style_cmap = ['RdBu_r', 'viridis', "YlGn"]

heatmap(data=df_1[df_1.columns.tolist()[:6]]
        , x_label=df_1.columns.tolist()[:6], y_label=[f'{dd} day' for dd in df_1.index]
        , style=style_cmap[1]
        , ax_title='user retention cnt heatmap', cbar_label='cbar')


heatmap(data=df_1[df_1.columns.tolist()[6:]]
        , x_label=df_1.columns.tolist()[:6], y_label=[f'{dd} day' for dd in df_1.index]
        , style=style_cmap[2]
        , ax_title='user retention rate heatmap'
        , cbar_label='cbar')

#################-------- part IV, visual AARRR--------##########################

aarrr = bData.groupby(by='action')['user_id'].nunique().to_frame().rename(columns={'user_id':'user_cnt'})
aarrr.reset_index(drop=False, inplace=True)
actionV = ['1_acquire', '2_activation', '3_retention', '4_revenue', '5_refer']
daDict = {}
for item in actionV:
    xixi = re.search('(?<=\d{1}_).*', item)[0]
    daDict[xixi] = item
aarrr['action'] = aarrr['action'].map(daDict)
aarrr.sort_values(by='action', inplace=True)
aarrr['hue'] = 0

horizontal_bar_chart(aarrr, 'action', 'user_cnt')


#laz = sns.barplot(data=aarrr, x= 'user_cnt', y='action', orient='h', color='olivedrab', saturation= 0.3)
#sns.color_palette("dark:#5A9", as_cmap=True)


#laz = sns.load_dataset(name='penguins')