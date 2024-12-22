import pandas as pd
import numpy as np
import datetime

def get_protfolio_net_value(start, end, protfolio_data, by='book_time_rank_first', head=True, amount=1000):
	protfolio1 = protfolio_data.loc[(protfolio_data['lag']==start)]
	protfolio1 = protfolio1.loc[(protfolio1['days_to_deadline']>0) & 
                          (protfolio1['state'] == 1) &
                          (protfolio1['trade_date'] >= (protfolio1['listed_date'] + datetime.timedelta(days=365)))] 
	protfolio1 = protfolio1.sort_values(by=['report_type', by])
	if head:
		protfolio1 = protfolio1.groupby('report_type').head(amount)
	else:
		protfolio1 = protfolio1.groupby('report_type').tail(amount)

	protfolio1_end = protfolio_data.loc[(protfolio_data['lag']==end), ['stock_symbol', 'report_type', 'trade_date', 'close']]
	protfolio1 = pd.merge(left=protfolio1[['stock_symbol', 'report_type', 'trade_date', 'close']], right=protfolio1_end, on=['stock_symbol', 'report_type'], how='left')

	protfolio1 = protfolio1.groupby('report_type')[['close_x', 'close_y']].mean().reset_index()
	protfolio1['rets'] = protfolio1['close_y'] / protfolio1['close_x']
	protfolio1['net_value'] = protfolio1['rets'].cumprod()
	return protfolio1

def max_dd(data, col):
	i = np.argmax((np.maximum.accumulate(data[col]) - data[col])) # 最大回撤结束的位置 最低的那个位置 np.argmax作用：取出数组中最大值对应的索引
	if i == 0:
		j = 0
	else:
		j = np.argmax(data[col][:i])  # 回撤开始的位置 最高的那个点
	maxdrawdown = data[col][j] - data[col][i] # 最大回撤
	maxdrawdown_rate = (data[col][j] - data[col][i]) / data[col][j] # 最大回撤率
	return maxdrawdown, maxdrawdown_rate, i, j