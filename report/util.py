import datetime
import pandas as pd
import exchange_calendars as ec # 交易日库

def get_trade_date(begin=None, end=None, calenders='XSHG'):
    '''
    获取交易日
    输入为datetime.date格式的起止日期，输出为dataframe
    '''
    calendar = ec.get_calendar(calenders)
    trade_date = pd.DataFrame({'trade_date': calendar.schedule.index})
    trade_date['trade_date'] = trade_date['trade_date'].dt.date
    if begin == None and end == None:
        return trade_date
    elif begin == None and end != None:
        return trade_date.loc[(trade_date['trade_date']<=end)]
    else:
        return trade_date.loc[(trade_date['trade_date']>=begin) & (trade_date['trade_date']<=end)]
    
trade_date = get_trade_date()

def trade_date_shift(date: datetime.date,
                     shift: int):
    '''
    获取指定日期的后几个交易日,非交易日为之前第一个交易日，shift>0为未来第几个交易日
    '''
    # 转到交易日，由交易日进行下一步操作
    if shift < 0:
        trade_date_shift = trade_date[trade_date['trade_date']<date].iloc[shift].values[0]
    else:
        trade_date_shift = trade_date[trade_date['trade_date']>=date].iloc[shift].values[0]
    return trade_date_shift