import pyrootutils
pyrootutils.setup_root(__file__, indicator="setup.py", pythonpath=True)

import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from report.evaluate.protfolio import get_protfolio_net_value

trade_date_lag = pq.read_table(f'./data/intermediate_results/trade_date_lag.parquet').to_pandas()
yysj_pro = pq.read_table(f'./data/intermediate_results/yysj_pro.parquet').to_pandas()
factor_data = pq.read_table(f'./data/intermediate_results/factor_data.parquet').to_pandas()[['stock_symbol', 'trade_date', 'is_suspended', 'is_st', 'listed_date', 'close']]

protfolio = pd.merge(left=trade_date_lag, right=yysj_pro, on=['stock_symbol', 'report_type'], how='left')
protfolio = pd.merge(left=protfolio, right=factor_data, on=['stock_symbol', 'trade_date'], how='left')

# 发报前买入早公告
protfolio1 = get_protfolio_net_value(-10, -1, protfolio, head=False)
# 发报前买入晚公告
protfolio2 = get_protfolio_net_value(-10, -1, protfolio, head=True)
# 发报后买入早公告
protfolio3 = get_protfolio_net_value(1, 10, protfolio, head=False)
# 发报后买入晚公告
protfolio4 = get_protfolio_net_value(1, 10, protfolio, head=True)

plt.figure(figsize=(20, 6))

# 创建折线图
plt.plot(protfolio1['report_type'], protfolio1['net_value'], linestyle='-', label='before_early')
plt.plot(protfolio2['report_type'], protfolio2['net_value'], linestyle='-', label='before_late')

plt.plot(protfolio3['report_type'], protfolio3['net_value'], linestyle='-', label='after_early')
plt.plot(protfolio4['report_type'], protfolio4['net_value'], linestyle='-', label='after_late')
# 旋转横坐标标签
plt.xticks(rotation=45)

# 添加标题和标签
plt.title('财报发布时间')
plt.xlabel('date')
plt.ylabel('net values')

# 添加图例
plt.legend()

# 显示图表
plt.grid(True) 
plt.show()