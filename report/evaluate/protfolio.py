import pandas as pd
import datetime

def get_protfolio_net_value(start, end, protfolio_data, by='days_to_deadline', head=True):
	protfolio1 = protfolio_data.loc[(protfolio_data['lag']==start)]
	protfolio1 = protfolio1.loc[(protfolio1['days_to_deadline']>0) & 
                          (protfolio1['is_suspended'] == False) &
                          (protfolio1['is_st'] == False) &
                          (protfolio1['trade_date'] >= (protfolio1['listed_date'] + datetime.timedelta(days=365)))] 
	protfolio1 = protfolio1.sort_values(by=['report_type', by])
	if head:
		protfolio1 = protfolio1.groupby('report_type').head(100)
	else:
		protfolio1 = protfolio1.groupby('report_type').tail(100)

	protfolio1_end = protfolio_data.loc[(protfolio_data['lag']==end), ['stock_symbol', 'report_type', 'trade_date', 'close']]
	protfolio1 = pd.merge(left=protfolio1[['stock_symbol', 'report_type', 'trade_date', 'close']], right=protfolio1_end, on=['stock_symbol', 'report_type'], how='left')

	protfolio1 = protfolio1.groupby('report_type')[['close_x', 'close_y']].mean().reset_index()
	protfolio1['rets'] = protfolio1['close_y'] / protfolio1['close_x']
	protfolio1['net_value'] = protfolio1['rets'].cumprod()
	return protfolio1