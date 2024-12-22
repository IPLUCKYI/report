import akshare as ak
import datetime
import pandas as pd
import pyarrow.parquet as pq

from pathlib import Path
import sys
import os
root_dir = str(Path(__file__).parent.parent)
sys.path.append(root_dir)
from report.util import trade_date_shift


def load_yysj_data(year):
    '''
    读取某一年的财务报告预约数据
    '''
    yysj_list = []
    try:
        # 当年一季报
        yysj_q1 = ak.stock_yysj_em(symbol="沪深A股", date=f"{year}0331")   
        yysj_q1['report_type'] = f'{year}0331'
        yysj_q1['start_day'] = datetime.date(year=year, month=4, day=1)
        yysj_q1['deadline'] = datetime.date(year=year, month=4, day=30)
        yysj_list.append(yysj_q1)
    except:
        pass
    try:
        # 当年半年报
        yysj_q2 = ak.stock_yysj_em(symbol="沪深A股", date=f"{year}0630")   
        yysj_q2['report_type'] = f'{year}0630'
        yysj_q2['start_day'] = datetime.date(year=year, month=7, day=1)
        yysj_q2['deadline'] = datetime.date(year=year, month=8, day=30)
        yysj_list.append(yysj_q2)
    except:
        pass
    try:
        # 当年三季报
        yysj_q3 = ak.stock_yysj_em(symbol="沪深A股", date=f"{year}0930")  
        yysj_q3['report_type'] = f'{year}0930'
        yysj_q3['start_day'] = datetime.date(year=year, month=10, day=1)
        yysj_q3['deadline'] = datetime.date(year=year, month=10, day=31)
        yysj_list.append(yysj_q3)
    except:
        pass
    try:
        # 去年年报
        yysj_q4 = ak.stock_yysj_em(symbol="沪深A股", date=f"{year-1}1231") 
        yysj_q4['report_type'] = f'{year-1}1231'
        yysj_q4['start_day'] = datetime.date(year=year, month=1, day=1)
        yysj_q4['deadline'] = datetime.date(year=year, month=4, day=30)
        yysj_list.append(yysj_q4)
    except:
        pass

    yysj = pd.concat(yysj_list, axis=0)
    yysj = yysj.rename(columns={'股票代码':'stock_symbol'})
    yysj = yysj.sort_values(by=['report_type','stock_symbol'])
    yysj = yysj.reset_index(drop=True)
    return yysj.drop(columns=['序号', '股票简称'])

def get_trade_date_lag(yysj, lag=0):
    trade_date_lag = yysj[['stock_symbol', 'report_type', '首次预约时间']].dropna()
    trade_date_lag['trade_date'] = trade_date_lag['首次预约时间'].apply(lambda x: trade_date_shift(x, lag))
    trade_date_lag['lag'] = lag
    return trade_date_lag[['stock_symbol', 'report_type', 'trade_date', 'lag']]

def calc_tech_factor(data):
    data = data.sort_values(by='trade_date')
    data['turnonver'] = data['volume'] / data['float_market_cap']
    data['vol5'] = data['turnonver'].rolling(5).mean()
    data['volt20'] = data['close'].rolling(20).std()
    data['mtm'] = data['close'] / data['close'].shift(10) - 1
    return data