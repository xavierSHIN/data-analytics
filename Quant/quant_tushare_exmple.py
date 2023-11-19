################################---------------------#############################------------------##########
# 了解 tushare data：
    # amt/vol = vwap
    # pct_chg = close-pre-close/pre-close
# problem set:
    # q1, 从2019年开始，每月月初买一首，每年年终卖出， 求收益
################################---------------------#############################------------------##########

from luffy._luffy_func import filePath, CONFIG_PATH
import tushare as ts
import numpy as np
import pandas as pd
import os, json, csv, re, datetime

## todo:: resample -- divide several parts
# current usage， # 拉取数据
with open(os.path.join(CONFIG_PATH, "tushare_token.json")) as tsFile:
    token = json.load(tsFile)['tushare_token']
pro = ts.pro_api(token)
fData = pro.us_daily(
    ts_code= 'FTCH'
    , fields=[
    "ts_code",
    "trade_date",
    "close",
    "open",
    "high",
    "low",
    "pre_close",
    "pct_change",
    "vol",
    "amount",
    "vwap",
    "change",
    "turnover_ratio",
    "total_mv",
    "adj_factor",
    "pe",
    "pb"]
)
#fData.to_csv(os.path.join(filePath, 'FTCH.csv'))
fData= pd.read_csv(os.path.join(filePath, 'FTCH.csv'))
# fData.columns[0]
fData.drop(columns=fData.columns[0], inplace=True)
#fData['trade_date'].apply(lambda q: pd.Timestamp(q))
#pd.to_datetime(fData['trade_date'])
fData['trade_date'] = fData['trade_date'].apply(lambda q: datetime.datetime.strptime(str(q), '%Y%m%d'))
#fData['trade_date'].apply(lambda q: pd.Timestamp(q))
fData.sort_values(by='trade_date', ascending=True, inplace=True)
#fData['trade_date'] = fData['trade_date'].dt.date
#fData['close'].shift(-1)
#fData.loc[0, ['amount', 'vol']][0]/fData.loc[0, ['amount', 'vol']][1]
#np.divide(fData['amount'], fData['vol'])
#min(fData.trade_date)
df_1 = fData.loc[ (fData.trade_date >= datetime.date(2019, 1, 1)) & (fData.trade_date < datetime.date(2023, 1, 1)), :]
#df_1.dtypes
df_1.index= df_1['trade_date']
#df_1.index
df_1['year_month'] = df_1['trade_date'].apply(lambda q: q.replace(day=1))
df_start = df_1.groupby('year_month', as_index=False).agg({'open': 'first'})
df_end = df_1.resample(rule='BA').asfreq()[['trade_date','close']]
df_end = df_end.loc[df_end.close.notnull(), :]
#df_end = df_end.dropna()
df_join = df_start.merge(df_end, how='left', left_on=df_start['year_month'].dt.year, right_on=df_end['trade_date'].dt.year)
df_join['revenue'] = df_join['close'] * 100
df_join['interest_mons'] = 13 - df_join['year_month'].dt.month
df_join['interests'] = np.power(1.02, df_join['interest_mons'])
df_join['cost'] = df_join['open'] * df_join['interests'] * 100
df_join['simple_cost'] = df_join['open'] * 100
df_join['margin'] = df_join['revenue']- df_join['cost']
df_join['simple_margin'] = 100 *(df_join['close'] - df_join['open'])
#profit_percent = sum(df_join['margin']) / sum(df_join['cost'])
ana = df_join.groupby('key_0').agg({'margin': 'sum', 'cost': 'sum', 'simple_margin': 'sum', 'simple_cost':'sum'})
#ana[['cum_margin', 'cum_simple_margin']] = ana[['margin', 'simple_margin']].apply(np.cumsum)
ana['prft_pert'] = ana['margin'] / ana['cost']
ana['simple_prft_pert'] = ana['simple_margin'] / ana['simple_cost']




##------------------------------------------###############################
df_monthly = df_1.resample("BMS").agg({'open': 'first'})
df_yearly = df_1.resample("BA").agg({'close': 'last'})[:-1]
cost_money = 0
hold = 0
for year in range(2019, 2023):
    # year = 2019
    cost_money += df_monthly[str(year)]['open'].sum() * 100
    hold += len(df_monthly[str(year)]['open']) * 100
    print(hold, cost_money)
    if year != 2023:
        cost_money -= df_yearly[str(year)]['close'][0] * hold
        hold = 0
    print(year, cost_money)
#cost_money -= hold * price_last
print(-cost_money)
# df_monthly['2001']
##------------------------------------------###############################

ARCHIVED = """
# training par starts
##t20, timeSeries
dateindex = pd.date_range(datetime.date(2023,1,1), datetime.date(2024,1,1), freq='M')
tada = pd.DataFrame({'price': np.random.randint(1000, 2000, 12)
                    ,'qty': np.random.randint(1000, 2000, 12)
                     }, index=[j.replace(day=1) for j in dateindex])
tada.resample('Q', label='left').sum()
def xixi(num):
    return num/4
tada.resample('W', ).apply(xixi).ffill()
# 1137/4
# traning par ends
# archived TS usage
kData = ts.get_k_data(code='601318')
len(kData)
kData.to_csv('601318.csv')"""
