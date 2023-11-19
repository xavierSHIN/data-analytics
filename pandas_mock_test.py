import pandas as pd
import datetime, random, os
import numpy as np
from luffy._luffy_func import filePath, plot_line_chart
#   user_id:用户ID
#   order_dt:购买日期
#   order_product:购买产品的数量
#   order_amount:购买金额

## todo:: plot diff charts in axe, fancy visual -- followup


## STEP I, xtracting data
with open(os.path.join(filePath, r'pandas_learn\课件\data\CDNOW_master.txt'), 'r') as foo:
    flines = foo.readlines()
aList = []
errorList = []
for line in flines:
    try:
        lList = line.split(' ')
        lList = [item for item in lList if item != '']
        if len(lList) ==4:
            aDict = {'user_id': lList[0], 'order_dt': lList[1], 'product_qty':lList[2], 'order_price': lList[3].strip('\n')}
            aDF = pd.DataFrame(aDict, index=[0])
            aList.append(aDF)
    except Exception as e:
        print(e)
        errorList.append(line)
        pass
print(len(errorList), len(aList), len(flines))
aData = pd.concat(aList, sort=True)
aData.reset_index(drop=True, inplace=True)
############-------##############----------------------------###################################################

aData = pd.read_csv(os.path.join(filePath, 'mock_data.csv')).reset_index(drop=True)
aData.drop(aData.columns[0], axis=1, inplace=True)
aData.dtypes

# STEP II data processing, nan drop/fill --> dedup --> dtypes --> extremes, stats

#aData = aData.loc[aData['user_id'].isnull(), :]
aData.loc[aData.isnull().any(axis=1), :]
#aData.drop_duplicates(keep='first')
aData = aData.loc[~aData.duplicated(),:].reset_index(drop=True)
aData['product_qty'] = aData['product_qty'].astype('int')
#set(aData['product_qty'])
aData['order_dt'] = aData['order_dt'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d'))
#aData['order_price'] = aData['order_price'].apply(lambda y: '0' if y== '' else y)
#aData = aData.loc[aData['order_price'] != '0', :].reset_index(drop=True)
aData['order_price'] = aData['order_price'].astype('float')
aData['order_month'] = aData['order_dt'].astype('datetime64[M]')
aData = aData.loc[aData['order_price'] != 0,:].reset_index(drop=True)
#aData.dtypes
#aData['order_dt'].dt.month
#aData.info()
aData.describe()
std_1 = aData['order_price'].std()* 2
std_2 = aData['product_qty'].std()* 2
aData.loc[aData['order_price'] > std_1,:].sort_values(by='order_price', ascending=False)
aData.loc[aData['product_qty'] > std_2,:].sort_values(by='product_qty', ascending=False)
############-------##############----------------------------###################################################


## STEP III, simple summary GROUPBY returns Series

aData['transaction_id'] = aData.index.tolist()
for col in ['transaction_id', 'user_id']:
    aData[col] = aData[col].astype('str')

# get monthly shopping total price, total qty
df_1 = aData[['order_price','order_month']].groupby('order_month').sum()
df_2 = aData.groupby('order_month')['product_qty'].sum().to_frame()
# get monthly transaction count
df_3 = aData.groupby('order_month')['transaction_id'].count().to_frame()
df_3_1 = aData.groupby('order_month')['user_id'].count().to_frame()
#aData['user_id'].unique()
# get hw many customers shopping each month
df_4 = aData.groupby('order_month')['user_id'].nunique().to_frame()
df_4_1 = aData[['order_month', 'user_id']].drop_duplicates(keep='first').groupby('order_month')['user_id'].count()
df_4_2 = aData.groupby('order_month')['user_id'].unique().apply(lambda x: len(set(x))).to_frame()
############-------##############----------------------------###################################################




## STEP IV, complex summary
# df.pivot_table(index='主客场',values='得分',columns='对手',aggfunc='sum',fill_value=0)
"""
df_6 = aData.pivot_table(index='product_qty', columns='order_month', values='order_price', aggfunc= 'mean')
df_6.fillna(value=0, inplace=True)
daDict = {}
daDict = {'x': list(df_6.columns)}
for index, row in df_6.iloc[:10,:].iterrows():
    #print(row)
    #type(row)
    daDict[index] = row.values.tolist()
daDict['xlabel'] = 'month'
daDict['ylabel'] = 'mean_order_price'
plot_line_chart(daDict)
"""
# relation btween order_price vs qty by user_id dimension, scatter
df_7 = pd.merge(
aData.groupby('user_id')['order_price'].sum().to_frame().rename(columns={'order_price': 'sum_price'})
, aData.groupby('user_id')['user_id'].count().to_frame().rename(columns={'user_id': 'count_txns'})
         , right_index=True, left_index=True
         , how = 'inner'
)
#df_7 = df_7.loc[df_7['count_txns'] <=10,:]
daDict = {}
#daDict = {'x': list(df_7.index)}
daDict['sum_price'] = df_7['sum_price'] #.tolist()
#daDict['count_txns'] = df_7['user_id'].tolist()
daDict['x'] = df_7['count_txns'] #.tolist()
daDict['xlabel'] = 'user_transactions_count'
daDict['ylabel'] = 'total_price_paid'
daDict['type'] = 'scatter'
daDict.keys()
plot_line_chart(daDict)


# histgram, only taks 1D array data
# np.random.randn(10, 3)
df_7.describe()
#[item for item in df_7['sum_price'] if item < df_7['sum_price'].quantile(.25)]
daDict = {}
#daDict['x'] = df_7['count_txns']
daDict['x'] = [item for item in df_7['sum_price'] if item < df_7['sum_price'].quantile(.25)]
#len(daDict['x'])
daDict['nbins'] = 20
daDict['xlabel'] = 'user_spend'
daDict['ylabel'] = 'count_of_user'
daDict['type'] = 'hist'
daDict.keys()
plot_line_chart(daDict)




############-------##############----------------------------###################################################
