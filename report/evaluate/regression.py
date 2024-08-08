import pandas as pd
import statsmodels.api as sm

def get_interval_rets(start, end, factor_data):
	'''
	计算区间收益
	'''
	factor_data1 = factor_data.loc[factor_data['lag'] == start, ['stock_symbol', 'report_type', 'trade_date', 'close']]
	factor_data2 = factor_data.loc[factor_data['lag'] == end, ['stock_symbol', 'report_type', 'trade_date', 'close']]
	returns = pd.merge(left=factor_data1, right=factor_data2, on=['stock_symbol', 'report_type'])
	returns[f'rets{start}_{end}'] = returns['close_y'] / returns['close_x'] - 1
	return returns[['stock_symbol', 'report_type', f'rets{start}_{end}']]

def reg(df, x, y):
	df = df.dropna()
	Y = df[y]
	X = df[x]
	X = sm.add_constant(X)
	model = sm.OLS(Y, X).fit()
	return model.summary()