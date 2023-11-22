from luffy._luffy_func import filePath, CONFIG_PATH
from luffy._pandas_seaborn_func import plot_line_chart
from jqdatasdk import *
import tushare as ts
import numpy as np
import pandas as pd
import os, json, csv, re, datetime


# problem:
# 1) every 30 days, refresh
# 2) only chose the bottom small cap stocks -- 3X
# 3) calc the position & profit
    # including fees
    # shift(3), weights, 100/hand
## todo:: 是否ST, 有效交易日历 -- check --freq = 'BM'

##------------------------------ partI, auth jqdata API -------------------------------------##
## json manual: https://blog.csdn.net/qq_46293423/article/details/105785007
## jdata manual: https://www.joinquant.com/help/api/doc?name=logon&id=9823

def initializer():

    with open(os.path.join(CONFIG_PATH, 'join_quant_creds.json'), 'r') as foo:
        details = foo.read()  # details: str, foo: textIo
        details_dict = json.loads(details)
    # auth('ID','Password')
    auth(details_dict['phone'], details_dict['pwd'])
    count_dict = get_query_count()
    print(count_dict['spare'])
    get_account_info()

    return


#all_securities.to_csv(os.path.join(filePath, 'all_securities.csv'))
#all_securities = pd.read_csv(os.path.join(filePath, 'all_securities.csv'))
#all_securities = all_securities.rename(columns={'Unnamed: 0': 'code'})
#all_securities.columns[0]

##------------------------------ partII, get small cap stock list -------------------------------------##
## query usage: https://www.joinquant.com/view/community/detail/433d0e9ed9fed11fc9f7772eab8d9376
# sqlalchemy.orm.query.Query
# https://www.youtube.com/watch?v=zQnJRl8BC6U

def get_smallcap_data():

    #all_securities = get_all_securities(types=['stock'], date=None)
    all_securities = pd.read_csv(os.path.join(filePath,'all_securities.csv'))
    #all_indexes= get_all_securities(types=['index'], date=None)
    len(all_securities)
    #all_securities.columns
    all_securities.rename(columns={'Unnamed: 0': 'code'}, inplace=True)
    q_2 = query(
        valuation.code,
        valuation.market_cap
    ).filter(
        valuation.market_cap.between(10, 30)
    ).order_by(
        valuation.market_cap.asc()
    )
    dateIndex = pd.date_range(start='2022-07-01', end='2023-11-01', freq='BM')
    aList = []
    for i_date in dateIndex:
        #print(i_date)
        tada = get_fundamentals(query_object=q_2, date=i_date)
        thatData = tada.iloc[:3, :]
        for index, row in thatData.iterrows():
            thatData.loc[index, 'fetch_date'] = i_date
        aList.append(thatData)
    fullData = pd.concat(aList, sort=False)
    fullData.sort_values(by='fetch_date', ascending=True, inplace=True)
    fullData = fullData.merge(all_securities[['code', 'display_name']], how='left', on='code')

    return fullData

##------------------------------ partIII, get stock price -------------------------------------##

def get_price_data(fullData):

    start_date = fullData['fetch_date'][0]
    end_date = fullData['fetch_date'].tolist()[-1]
    fullData['sell_code'] = fullData['code'].shift(3)
    field_list = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor'
        , 'high_limit', 'low_limit'
        , 'avg', 'pre_close', 'paused']
    df_index = get_price(security= '000300.XSHG'
                       , start_date= start_date
                       , end_date= end_date + datetime.timedelta(days=1, minutes=-1)
                       , frequency='daily'
                       , fields=field_list
                       , skip_paused=False, fq='pre'
                       , count=None, round=True)
    for index, row in fullData.iterrows():
        print(index, row['display_name'])
        df_buy = get_price(security=row['code']
                           , start_date=row['fetch_date']
                           , end_date=row['fetch_date'] + datetime.timedelta(days=1, minutes=-1)
                           , frequency='daily'
                           , fields=field_list
                           , skip_paused=False, fq='pre', count=None, round=True)
        fullData.loc[index, 'price'] = list(df_buy['open'])[0]
        try:
            df_sell = get_price(security=row['sell_code']
                           , start_date=row['fetch_date']
                           , end_date=row['fetch_date'] + datetime.timedelta(days=1, minutes=-1)
                           , frequency='daily'
                           , fields=field_list
                           , skip_paused=False, fq='pre', count=None, round=True)
            fullData.loc[index, 'sell_price'] = list(df_sell['close'])[0]
        except:
            fullData.loc[index, 'sell_price'] = 0
        df_st = get_extras(info='is_st'
                           , security_list=row['code']
                           , start_date=row['fetch_date']
                           , end_date=row['fetch_date'] + datetime.timedelta(days=1, minutes=-1)
                           , df=True, count=None)
        fullData.loc[index, 'is_st'] = df_st.iloc[0, 0]
        df_1 = fullData[['code', 'market_cap', 'display_name', 'fetch_date', 'price', 'is_st']]
        #df_1['type'] = ['buy'] * len(df_1)
        for index , row in df_1.iterrows():
            df_1.loc[index, 'type'] = 'buy'
        df_2 = fullData[['sell_code', 'fetch_date', 'sell_price']]
        #df_2['type'] = ['sell'] * len(df_2)
        for index , row in df_2.iterrows():
            df_2.loc[index, 'type'] = 'sell'
        df_2 = df_2.rename(columns={'sell_code': 'code', 'sell_price': 'price'})
        profitData = pd.concat([df_1, df_2], sort=True)
        profitData.sort_values(by=['fetch_date', 'type'], ascending=[True, False],inplace=True)
        profitData = profitData.iloc[3: -3, :] #.reset_index(drop=True)
        profitData.reset_index(drop=True, inplace=True)


    return profitData, df_index

##------------------------------ partIV, profit analysis -------------------------------------##

def get_profit_data(pData, cost, commission_rate):
    #commission = 0
    share = 0
    for index, row in pData.iterrows():
        if row['type'] == 'buy':
            shares, remainder = np.divmod(cost * (1-commission_rate), row['price'] * 100)
            share += shares * 100
            cost -= shares * 100 * row['price'] * (1+commission_rate)
        elif row['type'] == 'sell':
            cost += share * row['price']* (1- commission_rate)
            share = 0
        pData.loc[index, 'share'] = share
        pData.loc[index, 'cost'] = cost
        pData.loc[index, 'position'] = share * row['price']
        pData.loc[index, 'net_worth'] = share * row['price'] + cost

    return pData


if __name__ == '__main__':

    initializer()
    fullData = get_smallcap_data()
    profitData, df_index = get_price_data(fullData)
    aList = []
    N = 3
    for j in range(N):
        index_list = [i + j for i in range(0, len(profitData), 3)]
        pData_1 = profitData.iloc[index_list]
        cost_1 = 10000 / N
        commission_rate_1 = 0.003
        pData_1 = get_profit_data(pData_1, cost_1, commission_rate_1)
        aList.append(pData_1)
    aData = pd.concat(aList, sort=False)
    aData.sort_values(by=['fetch_date', 'type'], ascending=[True, False], inplace=True)
    bData = aData.loc[aData.type == 'sell', :].groupby('fetch_date').agg({'net_worth': lambda q: (sum(q)-10000)/10000})
    bData = bData.merge(df_index['open'], how='left', left_index=True, right_index=True).reset_index(drop=False)
    start_date = fullData['fetch_date'][0]
    start_point = df_index.loc[start_date, 'open']
    bData['benchmark'] = (bData['open'] - start_point)/bData['open']
    profit_pct = (sum(aData.loc[len(aData)-3:, 'net_worth']) -10000)/10000
    print(f"profit_pct: {round(profit_pct *100,2)} %")

##------------------------------ partV, two line plot -------------------------------------##
    tataDict = {
        'strategy': bData['net_worth']
        , 'benchmark': bData['benchmark']
        , 'x': bData['fetch_date']
        , 'xlabel': 'fetch_date'
        , 'ylabel': 'pct%'
        , 'type': 'plot'
        , 'nbins': ''
        , 'suptitle': 'smallcap_strategy_vs_benchmark'
        , 'title': ''
    }
    plot_line_chart(tataDict)










