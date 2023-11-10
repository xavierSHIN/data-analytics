
##todo:: 1), item_num 需要更精确的regex pattern --  check
##todo:: 2), float ranking: duration, plays --  check
##todo:: 3), 精品 与 非精品 tag -- check
##todo:: 4), 去重 -- check
##todo:: 5), directory as source_root in pycharm --check
##todo:: 6), to_sql mysql db localhost -- check
##todo:: 7), metaData setup
##todo:: 8), re-write file_to_db func
##todo:: 9), first line chart -- check
##todo:: 10), x-axis in week bracket -- check
##todo:: 11), put plot func in loop, save plt pic? --TBD
##todo:: 12), cmts api digging -- check
##todo:: 13), refine the loop END ??
##todo:: 14), user_id --> following
## todo:: xaviershin repo


import pandas as pd
import numpy as np
import requests, os, re, time, datetime
import openpyxl, xlsxwriter
from bs4 import BeautifulSoup
from luffy._luffy_func import parse_wan, db #, Cumulative, plot_line_chart
from luffy._luffy_func import filePath, snake_case
from luffy.pandas_seaborn_func import plot_line_chart, heatmap, horizontal_bar_chart
## BS manual
## https://blog.csdn.net/qq_44402069/article/details/107690527

aStr = """pager = 155
user_id = 14302642
fm = 222220
pattern =  '(?<=北京话事人)(\d+)$'
"""
# pd.Timestamp(1691039853161)
# 使用SESSION可以带着COOKIEs



## Step1: main body, extracting & processing data
def get_lizhi_info(user_id,
                   fm,
                   pattern,
                   pager):


    lizhi_url = f'https://www.lizhi.fm/user/{user_id}/p/{pager}.html'
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.46'
    }
    res = requests.session()
    lizhi = res.get(lizhi_url, headers=headers)
    bs_2 = BeautifulSoup(lizhi.text, 'html.parser')
    soundList = [yy for yy in bs_2.findAll(name='li')]
    # len(soundList) , soundList[13], len(aList), lala = pd.DataFrame(aList[8])
    # , item = soundList[13]
    #print(soundList)
    aList = []
    for item in soundList:
        try:
            itemDict = {}
            item_id = re.search('(?<=data-id=)("\d+")(?= data-islistenfirst)', str(item))[0].strip('"')
            paid_item = re.search('(?<=data-payflag=)("\d{1}")', str(item))[0].strip('"')
            #print(paid_item)
            item_index = item.find_all(name = 'span', attrs= {'class': 'radio-list-item-index'})[0].string
            item_title = item.find_all(name='p', attrs={'class': 'audioName'})[0].string
            try:
                item_num = re.search(pattern, item_title)[0]
            except:
                item_num = 'xxx'
            detail_res = res.get(url=f'https://www.lizhi.fm/{fm}/{item_id}', headers=headers)
            bs_3 = BeautifulSoup(detail_res.text, 'html.parser')
            cnt_list = [jj.string for jj in bs_3.find_all(name='span', attrs={'class':'text'})]
            plays = parse_wan(cnt_list[0])
            downloads = parse_wan(cnt_list[1])
            audio_time_str = item.find_all(name='p', attrs={'class': 'aduioTime'})[0].string
            num_comment = re.search('(\d+)(?=评论$)', audio_time_str)[0]
            item_date = re.search('\d{4}-\d{2}-\d{2}', audio_time_str)[0]
            item_duration = item.find_all(name='div', attrs={'class': 'right duration'})[0].string.strip("'")
            item_duration = int(item_duration.split("'")[0]) + float(item_duration.split("'")[1])/60
            item_duration = round(item_duration, 2)
            itemDict['item_index'] = item_index
            itemDict['item_title'] = item_title
            itemDict['paid_item'] = paid_item
            itemDict['item_num'] = item_num
            itemDict['item_id'] = item_id
            itemDict['item_date'] = item_date
            itemDict['num_comment'] = num_comment
            itemDict['item_duration'] = item_duration
            itemDict['plays'] = plays
            itemDict['downloads'] = downloads
            #print(itemDict)
            aList.append(pd.DataFrame(itemDict, index=[0]))
        except:
            #print(e)
            pass
    soundData = pd.concat(aList, sort=False).reset_index(drop=True)
    for col in ['plays', 'downloads', 'item_duration', 'num_comment']:
        soundData[col] = soundData[col].astype('float')
    # soundData.dtypes
    soundData['paid_item'] = soundData['paid_item'].map({'1': True, '0': False})
    soundData['item_id'] = soundData['item_id'].astype('str')
    soundData['item_date'] = soundData['item_date'].apply(lambda q: datetime.datetime.strptime(q, '%Y-%m-%d'))
    soundData['item_date'] = soundData['item_date'].dt.date
    soundData['week_num'] = soundData['item_date'].apply(lambda q:  str(q.isocalendar()[0]) + 'W' + str(q.isocalendar()[1]))
    # soundData['item_duration'] = list(map(lambda x: round(x, 2), soundData['item_duration'].tolist()))
    # type( datetime.date( spineData['iso_week_num'][0].isocalendar()[1] )

    return soundData




def get_cmt_details(
        item_id
    ):

    #item_id = '2506153650743681542'
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.46'
    }
    page = 1
    lizhi_url = f'https://www.lizhi.fm/api/program/comments/{item_id}/{page}'
    first_info = requests.get(lizhi_url, headers=headers)
    first_infoDict = first_info.json()
    total_pager = first_infoDict['pageTotal']
    aList = []
    for page in range(1, total_pager+1):
        print(f"page: {page}")
        laz = pd.DataFrame()
        lizhi_url = f'https://www.lizhi.fm/api/program/comments/{item_id}/{page}'
        info = requests.get(lizhi_url, headers=headers)
        infoDict = info.json()
        for item in infoDict['list']:
            newDict = {k: v for k,v in item.items() if k != 'user'}
            newData = pd.DataFrame(newDict, index=[0])
            newData['user_id'] = item['user']['id']
            newData['user_name'] = item['user']['name']
            laz = laz.append(newData)
        laz['item_id'] = item_id
        laz['page'] = page
        aList.append(laz)
    cmtData = pd.concat(aList, sort=False)
    newCols = ['item_id', 'page', 'user_id', 'user_name', 'createTime', 'content'
                    ,'id',  'laud', 'laudCount', 'replyIsDel', 'startTime'
                    , 'time', 'toContent', 'toId', 'toUserId','toUserName']
    cmtData = cmtData[newCols]
    cmtData.sort_values(by='createTime', inplace=True)
    cmtData.reset_index(drop=True, inplace=True)

    return cmtData





def data_modeling(dataSet
                  , paid):
    # alist = []
    # for j in range(2015, 2024, 1):
    #    for i in range(1, 53):
    #        thatItem = f"{j}W{i}"
    #        alist.append(thatItem)
    # dataSet = laz
    # paid =True
    #dataSet = pd.DataFrame()
    first_day = min(dataSet['item_date']) if 'item_date' in dataSet.columns else datetime.date(2015,1,1)
    last_day = max(dataSet['item_date']) if 'item_date' in dataSet.columns else datetime.date.today()
    weekIndex= pd.date_range(start= first_day, end= last_day, freq='W')
    spineData = pd.DataFrame({'iso_week_day': weekIndex.tolist()})
    spineData['iso_month'] = spineData['iso_week_day'].dt.month
    spineData['iso_year'] = spineData['iso_week_day'].dt.year
    spineData['iso_week_day'] = spineData['iso_week_day'].dt.date
    spineData['week_num'] = spineData['iso_week_day'].apply(lambda q: str(q.isocalendar()[0]) + 'W' + str(q.isocalendar()[1]))
    dataSet = dataSet.loc[dataSet['paid_item'] == paid, :]
    grouped = dataSet[['week_num', 'plays']].groupby(by='week_num', as_index=False).sum().sort_values(by='week_num')
    spineData = spineData.merge(grouped, on='week_num', how='left')
    spineData = spineData.fillna(value={'plays': 0})
    # spineData['iso_year'] = spineData['iso_week_day'].apply(lambda x: re.search('^\d{4}', str(x))[0] if re.search('^\d{4}', str(x)) is not None else '0')
    spineData['iso_week'] = spineData['week_num'].apply(lambda y: re.search('(?<=W)\d+$', str(y))[0] if re.search('(?<=W)\d+$', str(y)) is not None else '0')
    for i in ['week', 'month', 'year']:
        spineData[f'iso_{i}'] = spineData[f'iso_{i}'].astype(int)
    spineData.sort_values(by='iso_week_day', inplace=True)
    #spineData.dtypes
    bigList = []
    legendList = []
    # rr= '2023'
    for rr in range(2017, 2024):
        legendList.append(str(rr))
        thatYear = spineData.loc[spineData['iso_year'] == rr, :]
        thatYear['cumsum_plays'] = thatYear['plays'].transform(np.cumsum)
        bigList.append(thatYear)
    newSpine = pd.concat(bigList, sort=True)

    return newSpine




if __name__ == '__main__':

    kwargs_list = [
        {'user_name': '北京话事人'
            , 'user_id': '14302642'
            , 'fm': '222220'
            , 'pattern': '(?<=北京话事人)(\d+)$'
         }
        #, {'user_name': '糖蒜广播'
         #   , 'user_id': '332'
         #   , 'fm': '13461'
         #, 'pattern': '(?<=VOL)(.*)$'
         #}
    ]
    xu_mysql = db('xu_mysql')


# part I, get catelog data-----------------------------------------------------------###########################
    for kitem in kwargs_list:
        laz = pd.DataFrame()
        for pager in range(1, 200):
            kitem['pager'] = pager
            kwargs = {key: val for key, val in kitem.items() if key != 'user_name'}
            try:
                thatData = get_lizhi_info(**kwargs)
                if not thatData.empty:
                    laz = laz.append(thatData).reset_index(drop=True)
                    print(f"{kitem['user_name']} /page - {pager} finished !")
            except Exception as e:
                print(e)
                #time.sleep(30)
                pass
        laz = laz.drop('item_index', axis=1)
        laz.drop_duplicates(keep='first', inplace=True)
        laz.to_excel(os.path.join(filePath, f"{kitem['user_name']}_OG.xlsx")
                     , sheet_name=kitem['user_name']
                     , index=False)
        #len(laz) , len(dataSet)


# part II, get comment details-----------------------------------------------------------###########################
    dtype_dict = {'item_title': 'str',
            'paid_item': 'str',
            'item_num': 'str',
            'item_id': 'str',
            #'item_date': dtype('<M8[ns]'),
            'num_comment': 'float64',
            'item_duration': 'float64',
            'plays': 'float64',
            'downloads': 'float64',
            'week_num': 'str'}
    laz = pd.read_excel(os.path.join(filePath, '北京话事人_OG.xlsx'), sheet_name='北京话事人', dtype=dtype_dict)
    # laz.dtypes
    bList = []
    errorList = []
    run_page = 0
    rowCnt = 0
    for index, row in laz.iloc[20:, :].iterrows():
        try:
            print(f"fetching: {row['item_id']} -- {row['item_title']} ")
            rowData = get_cmt_details(item_id=row['item_id'])
            rowCnt += len(rowData)
            bList.append(rowData)
        except Exception as e:
            errorList.append({'item_id': e})
            pass
        if rowCnt >= 500:
            print(f"----------------------------------------------------rowcnt:{rowCnt}, runpage:{run_page}")
            tada = pd.concat(bList, sort=False)
            tada.sort_values(by=['item_id', 'createTime'], inplace=True)
            tada.reset_index(drop=True, inplace=True)
            try:
                with pd.ExcelWriter(os.path.join(filePath, "OG_OG.xlsx"), mode='a', engine='openpyxl') as writer:
                    tada.to_excel(writer
                              , sheet_name=f"P{run_page}"
                              , index=False)
            except openpyxl.utils.exceptions.IllegalCharacterError:
                with pd.ExcelWriter(os.path.join(filePath, "OG_OG_error.xlsx"), mode='w', engine='xlsxwriter') as ewriter:
                    tada.to_excel(ewriter
                                , sheet_name=f"P{run_page}"
                                , index=False)
            run_page += 1
            rowCnt = 0
            bList = []


# part III, analysis-----------------------------------------------------------###########################
    laz['paid_item'] = laz['paid_item'].map({'True': 1, 'False': 0})
    laz['paid_item'] = laz['paid_item'].astype('bool')
    parseData = data_modeling(dataSet=laz, paid=True)
    idx = pd.DatetimeIndex(parseData['iso_week_day'])
    #idx
    parseData.set_index(idx, inplace=True)
    df_1 = parseData.pivot_table(index='iso_week', columns='iso_year', values='plays', aggfunc='sum')
    df_1 = df_1.fillna(value=0)
    df_2 = parseData.pivot_table(index='iso_month', columns='iso_year', values='plays', aggfunc='sum')
    df_2 = df_2.fillna(value=0)

    df_3 = parseData.resample('M').agg({'plays': 'sum', 'iso_year': 'first'})
    df_3.index = [mm.month for mm in df_3.index]
    df_3.reset_index(drop=False,  inplace=True)
    df_3 = df_3.pivot_table(index='index', columns='iso_year', values='plays', aggfunc='sum')
    df_3 = df_3.fillna(value=0)


    style_cmap = ['RdBu_r', 'viridis', "YlGn"]
    heatmap( data=df_1.T
            , x_label=[f'W{dd}' for dd in df_1.index]
            , y_label=df_1.columns.tolist()
            , style=style_cmap[0]
            , ax_title='BJ plays heatmap'
            , cbar_label='cbar'
            )


    heatmap( data=df_2.T
            , x_label=[f'M{mon}' for mon in df_2.index]
            , y_label=df_2.columns.tolist()
            , style=style_cmap[1]
            , ax_title='BJ plays heatmap'
            , cbar_label='cbar'
            )


    heatmap( data= df_3
            , x_label= df_3.columns.tolist()
            , y_label= [f'M{mon}' for mon in df_3.index]
            , style=style_cmap[2]
            , ax_title='BJ plays heatmap'
            , cbar_label='cbar'
            )

## part IV -- analysis cmnts data
    cmntsData = pd.DataFrame()
    for j in range(60):
        #with open(os.path.join(filePath, 'OG_OG.xlsx'), 'r', encoding='gbk') as foo:
        mario = pd.read_excel(os.path.join(filePath, 'OG_OG.xlsx'), sheet_name=f'P{j}',  index=False)
        cmntsData = cmntsData.append(mario)
        len(cmntsData)
    cmntsData.to_excel(os.path.join(filePath, '北京话事人_评论.xlsx'), sheet_name='北京话事人')
    cmntsData.columns = [snake_case(col) for col in cmntsData.columns]
    cmntsData['user_id'] = cmntsData['user_id'].astype('str')
    #postD = cmntsData['user_name'].value_counts().to_frame()
    #postD.iloc[1,:]['content']
    cmntsData['is_gift_msg'] = cmntsData['content'].apply(lambda p: True if re.fullmatch('.*给 北京话事人 送了.*', p) else False)
    cmntsLean = cmntsData.loc[cmntsData['is_gift_msg'] == False, :]
    #postD = cmntsLean.groupby(by='user_name').agg({'user_id': 'count', 'content': lambda q: ','.join(q)})
    #postD.sort_values(by='user_id', ascending=False, inplace=True)
    postD = cmntsLean.pivot_table(index='user_name', values='user_id', aggfunc='count')
    postD.reset_index(drop=False, inplace=True)
    postD.sort_values(by='user_id', ascending=False, inplace=True)
    postD = postD.iloc[:30, :]
    horizontal_bar_chart(data=postD, category='user_name', count='user_id')


    giftsData = cmntsData.loc[cmntsData['is_gift_msg'] == True, :]
    giftsData['gift'] = giftsData[['content']].apply(lambda x: re.search('(?<=北京话事人 送了)(.*)', str(x))[0], axis=1)
    gList = giftsData.groupby(by='user_name', as_index=False).agg({'user_id': 'count'}).sort_values(by='user_id', ascending=False)['user_name'][:20]
    giftsData = giftsData.loc[giftsData['user_name'].isin(gList), :]
    giftD = giftsData.pivot_table(index='user_name', columns='gift', values= 'user_id', aggfunc='count')
    giftD = giftD.fillna(value=0)
    heatmap(data= giftD
            , x_label= giftD.columns.tolist()
            , y_label= giftD.index.tolist()
            , style= style_cmap[1]
            , ax_title= 'BJ gifts heatmap'
            , cbar_label= 'cbar'
            )

    ## dedup part -- raw dataframe has dup rows on item_title
    #waz = laz[laz.columns.tolist()[1:]]
    #waz.drop_duplicates(subset='item_title', keep='first', inplace=True)
    #print(len(set(laz['item_title'])), len(waz), len(laz))
    #laz.loc[laz.duplicated('item_title') == True,:]
    ## if goes to EXCEL

"""
with open(os.path.join(filePath, 'response.txt'), 'w', encoding='utf-8') as foo:
    foo.write(detail_res.text)
wooz = pd.read_excel(os.path.join(filePath, '北京话事人.xlsx'), sheet_name='北京话事人_2 (2)')
"""

