import tushare as ts
import numpy as np
import pandas as pd
import os, json, datetime
from pandas.plotting import register_matplotlib_converters
from luffy._pandas_seaborn_func import plot_line_chart
from luffy._luffy_func import filePath, CONFIG_PATH

register_matplotlib_converters()
## ------------------------------------------------------##############################
# problem set:
    # q1, 如何判断金叉死叉 --> shift & shift & bool
    # q2, 从2019年开始，10000本金，每个月金叉全买入，每个死叉全卖出, 求收益
    # PB ratio, price to book
## ------------------------------------------------------##############################

def cross_analysis(fData
                , stock_code):

    # part1, depict moving avg plot lookback 5day, 10day, 30day
    try:
        fData['trade_date'] = fData['trade_date'].apply(lambda q: datetime.datetime.strptime(str(q), '%Y-%m-%d'))
    except:
        fData['trade_date'] = fData['trade_date'].apply(lambda q: datetime.datetime.strptime(str(q), '%Y%m%d'))
    # adj_date = fData.loc[fData['adj_factor'] != 1,:][['trade_date', 'adj_factor']].tolist()[0]
    if stock_code == 'AMZN':
        fData.loc[fData.trade_date <= '2022-06-03', 'adj_factor'] = 0.05
    elif stock_code == 'TSLA':
        fData.loc[fData.trade_date <= '2022-08-24', 'adj_factor'] = 0.3333
        fData.loc[fData.trade_date <= '2020-08-28', 'adj_factor'] = 0.07

    fData = fData.loc[fData.trade_date.dt.year >= 2018, :].reset_index(drop=True)
    fData['close'] = fData['close'] * fData['adj_factor']
    # fData.columns
    # fData.to_csv(os.path.join(filePath, f'{stock_code}_full_metrics .csv'))

    for index, row in fData.iterrows():
        # index= 0
        try:
            lookback_5 = np.mean(fData.iloc[index:index + 5]['close'])
            lookback_10 = np.mean(fData.iloc[index:index + 10]['close'])
            lookback_30 = np.mean(fData.iloc[index:index + 30]['close'])
            fData.loc[index, '5day_back'] = lookback_5
            fData.loc[index, '10day_back'] = lookback_10
            fData.loc[index, '30day_back'] = lookback_30
        except:
            pass

    # fData.iloc[1:5+1]['close'].mean()
    xlabel = 'trade_date'
    ylabel = 'close_k'
    dataDict = {
        '5k': fData['5day_back']
        , '10k': fData['10day_back']
        , '30k': fData['30day_back']
        , 'x': fData['trade_date']
        , 'xlabel': xlabel
        , 'ylabel': ylabel
        , 'type': 'plot'
        , 'nbins': ''
        , 'suptitle': stock_code
        , 'title': ''
    }
    plot_line_chart(dataDict)

    ## part II, dead_cross, gold_cross, and profit analysis
    # gold_cross, 5dayK > 30dayK, +1 same, -1 opposite

    df_gold = fData.loc[
              (fData['5day_back'] > fData['30day_back']) & \
              (fData['5day_back'].shift(-1) < fData['30day_back'].shift(-1)) & \
              (fData['5day_back'].shift(1) > fData['30day_back'].shift(1))
    , :]
    df_gold['type'] = 'gold'
    df_dead = fData.loc[
              (fData['5day_back'] < fData['30day_back']) & \
              (fData['5day_back'].shift(-1) > fData['30day_back'].shift(-1)) & \
              (fData['5day_back'].shift(1) < fData['30day_back'].shift(1))
    , :]
    df_dead['type'] = 'dead'
    df_1 = df_gold.append(df_dead)
    df_1.sort_values(by='trade_date', ascending=True, inplace=True)
    # df_1.reset_index(drop=True, inplace=True)
    min_gold = df_gold.trade_date.min()
    df_1 = df_1.loc[df_1.trade_date >= min_gold, :].reset_index(drop=True)

    # part III, profit calc
    cost = 10000  # money
    position = 0  # share_values
    share = 0
    for index, row in df_1.iterrows():
        if row['type'] == 'gold':
            buys, remainder = np.divmod(cost, row['close'])
            cost -= buys * row['close']
            share += buys
            position = share * row['close']
        elif row['type'] == 'dead':
            cost += share * row['close']
            share = 0
            position = 0
        # print(row['type'], share, position, cost, sep='----')
        df_1.loc[index, 'share'] = share
        df_1.loc[index, 'position'] = position
        df_1.loc[index, 'cost'] = cost
    df_1['net_worth'] = df_1['cost'] + df_1['position']
    col_list = ['ts_code', 'trade_date', 'close', 'type', 'share', 'position', 'cost', 'net_worth'
        , 'open', 'high', 'low', 'pre_close',
                'change', 'pct_change', 'vol', 'amount', 'vwap', 'adj_factor',
                'turnover_ratio', 'total_mv', 'pe', 'pb', '5day_back', '10day_back',
                '30day_back']
    df_1 = df_1[col_list]
    # each year's net_worth
    nData = df_1.loc[df_1.trade_date.dt.year != df_1.trade_date.shift(-1).dt.year, ['trade_date', 'net_worth']]
    print(nData)
    net_worth = nData['net_worth'].tolist()[-1]
    print(f'    % profit margin: {round((net_worth - 10000) / 10000, 4) * 100}%')
    fData['date_mark'] = 'normal_day'
    fData.loc[fData.trade_date.isin(df_1.trade_date), 'date_mark'] = 'key_day'

    ## part IV, plot cross day volume, result: not significant
    fData['fill'] = fData[['date_mark', 'vol']].apply(lambda q: q[1] if q[0] == 'key_day' else 0, axis=1)
    xlabel = 'trade_date'
    ylabel = 'vol'
    tataDict = {
        'normal_day': fData['vol']
        , 'key_day': fData['fill']
        , 'x': fData['trade_date']
        , 'xlabel': xlabel
        , 'ylabel': ylabel
        , 'type': 'plot'
        , 'nbins': ''
        , 'suptitle': f'{stock_code}_vol_chg_chart'
        , 'title': ''
    }
    plot_line_chart(tataDict)
    # df_1.loc[df_1.vol == max(df_1.loc[df_1.trade_date.dt.year==2023,'vol']),:]

    return df_1, nData


if __name__ == '__main__':

    # partO, extracting data
    field_list = [
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
    with open(os.path.join(CONFIG_PATH, "tushare_token.json")) as tsFile:
        token = json.load(tsFile)['tushare_token']
    pro = ts.pro_api(token)
    stock_list = ['FTCH', 'AMZN', 'TSLA']
    stock_code = stock_list[0]
    try:
        fData = pro.us_daily(
            ts_code=stock_code
            , fields=field_list
        )
    except:
        fData = pd.read_csv(os.path.join(filePath, f'{stock_code}_full_metrics .csv'))
        fData.drop(columns='Unnamed: 0', inplace=True)

    position_sheet, profit_sheet = cross_analysis(fData, stock_code)
    # fData.dtypes
