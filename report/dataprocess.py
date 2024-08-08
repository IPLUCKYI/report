import pyrootutils
pyrootutils.setup_root(__file__, indicator="setup.py", pythonpath=True)

from report.load.load_report_time import load_yysj_data, get_trade_date_lag

import os
import multiprocessing as mp
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from functools import partial

if __name__ == '__main__':
# --------------------------------读取预约数据并存储为yysj.pq--------------------------------
    print('正在读取预约数据')
    yysj_file = f'./data/intermediate_results/yysj.parquet'
    if os.path.isfile(yysj_file):
        yysj = pq.read_table(yysj_file).to_pandas()
    else:
        with mp.Pool() as pool:
            yysj = pool.map(load_yysj_data, range(2007,2025))
        yysj = pd.concat(yysj).reset_index(drop=True)
        yysj.to_parquet(yysj_file)
    print('读取预约数据完成')
# --------------------------------生成预约时间衍生指标并存储为yysj_pro.pq--------------------------------
    print('正在生成预约时间衍生指标')
    yysj_pro_file = f'./data/intermediate_results/yysj_pro.parquet'
    if os.path.isfile(yysj_pro_file):
        yysj_pro = pq.read_table(yysj_pro_file).to_pandas()
    else:
        yysj_pro = yysj.dropna(subset=['stock_symbol', '首次预约时间', '实际披露时间']).reset_index(drop=True).copy()
        # 生成最终预约时间
        yysj_pro['latest_book_date'] = np.nan
        yysj_pro['latest_book_date'] = yysj_pro['latest_book_date'].fillna(yysj_pro['三次变更日期'])
        yysj_pro['latest_book_date'] = yysj_pro['latest_book_date'].fillna(yysj_pro['二次变更日期'])
        yysj_pro['latest_book_date'] = yysj_pro['latest_book_date'].fillna(yysj_pro['一次变更日期'])
        yysj_pro['latest_book_date'] = yysj_pro['latest_book_date'].fillna(yysj_pro['首次预约时间'])
        # 预约时间排序
        yysj_pro['book_time_rank_first'] = yysj_pro.groupby('report_type')['首次预约时间'].rank(ascending=True, pct=True, method='min')
        yysj_pro['book_time_rank_last'] = yysj_pro.groupby('report_type')['latest_book_date'].rank(ascending=True, pct=True, method='min')
        yysj_pro['report_time_rank_last'] = yysj_pro.groupby('report_type')['实际披露时间'].rank(ascending=True, pct=True, method='min')
        yysj_pro['days_to_deadline'] = (yysj_pro['deadline'] - yysj_pro['首次预约时间']).apply(lambda x: x.days)
        yysj_pro = yysj_pro[['stock_symbol', 'report_type', 'book_time_rank_first', 'book_time_rank_last', 'report_time_rank_last', 'days_to_deadline']]
        yysj_pro.to_parquet(yysj_pro_file)
    print('生成预约时间衍生指标完成')
# --------------------------------生成不同滞后期的对应时间并存储为trade_date_lag.pq--------------------------------
    print('正在生成不同滞后期')
    trade_date_lag_file = f'./data/intermediate_results/trade_date_lag.parquet'
    if os.path.isfile(trade_date_lag_file):
        trade_date_lag = pq.read_table(trade_date_lag_file).to_pandas()
    else:
        with mp.Pool() as pool:
            partial_func = partial(get_trade_date_lag, yysj)
            trade_date_lag = pool.map(partial_func, range(-30,31))
            trade_date_lag = pd.concat(trade_date_lag).reset_index(drop=True)
        trade_date_lag.to_parquet(trade_date_lag_file)
    print('生成不同滞后期处理完成')
# --------------------------------处理财报数据并存储为report_data.pq--------------------------------
    print('正在处理财报数据')
    report_data_file = f'./data/intermediate_results/report_data.parquet'
    if os.path.isfile(report_data_file):
        report_data = pq.read_table(report_data_file).to_pandas()
    else:
        report_data = pd.read_csv(f'./data/raw_data/report_data.csv')
        report_data['stock_symbol'] = report_data['order_book_id'].str[:6]
        report_data['report_type'] = report_data['quarter'].str.replace(r'(\d{4})q1', r'\g<1>0331', regex=True)
        report_data['report_type'] = report_data['report_type'].str.replace(r'(\d{4})q2', r'\g<1>0630', regex=True)
        report_data['report_type'] = report_data['report_type'].str.replace(r'(\d{4})q3', r'\g<1>0930', regex=True)
        report_data['report_type'] = report_data['report_type'].str.replace(r'(\d{4})q4', r'\g<1>1231', regex=True)
        report_data = report_data[['stock_symbol', 'report_type', 'total_liabilities', 'net_profit', 'total_assets', 'total_equity', 'basic_earnings_per_share']]
        report_data.to_parquet(report_data_file)
    print('财报数据处理完成')
# --------------------------------处理股票信息数据并存储为stk_info.pq--------------------------------
    print('正在处理股票信息数据')
    stk_info_file = f'./data/intermediate_results/stk_info.parquet'
    if os.path.isfile(stk_info_file):
        stk_info = pq.read_table(stk_info_file).to_pandas()
    else:
        stk_info = pd.read_csv(f'./data/raw_data/stk_info.csv')
        stk_info['stock_symbol'] = stk_info['order_book_id'].str[:6]
        stk_info['listed_date'] = pd.to_datetime(stk_info['listed_date'], errors='coerce').dt.date
        stk_info = stk_info.drop(columns='order_book_id')
        stk_info.to_parquet(stk_info_file)
    print('股票信息数据处理完成')
# --------------------------------处理因子数据并存储为factor_data.pq--------------------------------
    print('正在处理因子数据')
    factor_data_file = f'./data/intermediate_results/factor_data.parquet'
    if os.path.isfile(factor_data_file):
        factor_data = pq.read_table(factor_data_file).to_pandas()
    else:
        factor_data_file_list = [f'./data/raw_data/factor_data1.csv',
                            f'./data/raw_data/factor_data2.csv',
                            f'./data/raw_data/factor_data3.csv',]
        factor_data_list = []
        for factor_data_1 in factor_data_file_list:
            factor_data = pd.read_csv(factor_data_1)
            factor_data['stock_symbol'] = factor_data['order_book_id'].str[:6]
            factor_data['trade_date'] = pd.to_datetime(factor_data['date']).dt.date
            factor_data.drop(columns=['order_book_id', 'date'], inplace=True)
            factor_data_list.append(factor_data)
        factor_data = pd.concat(factor_data_list)
        # 拼接上市日期
        factor_data = pd.merge(left=factor_data, right=stk_info[['stock_symbol', 'listed_date']], on='stock_symbol', how='left')
        factor_data.to_parquet(factor_data_file)
    print('因子数据处理完成')
# --------------------------------处理指数数据并存储为index_data.pq--------------------------------
    print('正在处理指数数据')
    index_data_file = f'./data/intermediate_results/index_data.parquet'
    if os.path.isfile(index_data_file):
        index_data = pq.read_table(index_data_file).to_pandas()
    else:
        index_data = pd.read_csv(f'./data/raw_data/index_data.csv')
        index_data['stock_symbol'] = index_data['order_book_id'].str[:6]
        index_data['trade_date'] = pd.to_datetime(index_data['date']).dt.date
        index_data.drop(columns=['order_book_id', 'date'], inplace=True)
        index_data.to_parquet(index_data_file)
    print('指数数据处理完成')


