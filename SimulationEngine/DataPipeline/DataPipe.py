import pandas as pd
from Utils import get_price_data, get_fundamental_data
import os

class DataPipe:
    def __init__(self,
                 period_start,
                 period_end,
                 sep_tickers,
                 sfp_tickers,
                 data_save_name,
                 all_prev=False,
                 signal_generators = []):
        self.period_start = period_end
        self.period_end = period_end
        self.sep_tickers = sep_tickers
        self.sft_tickers = sfp_tickers
        sep_price_data = get_price_data(sep_tickers, period_start, period_end, table='SEP')
        sfp_price_data = get_price_data(sfp_tickers, period_start, period_end, table='SFP')
        dates = sep_price_data.index.get_level_values('calendardates').unique()
        fund_data = get_fundamental_data(dates, sep_tickers)
        price_data = pd.concat([sep_price_data, sfp_price_data])
        self.all_prev = all_prev
        os.mkdir(data_save_name)

        TotalSignals = pd.DataFrame()
        for sg in signal_generators:
            signal = sg.generate(price_data, fund_data)
            signal_name = sg.name
            TotalSignals[signal_name] = signal

        fund_data.to_csv(f'{data_save_name}/fund_data.csv')
        price_data.to_csv(f'{data_save_name}/price_data.csv')
        self.fund_data = fund_data
        self.price_data = price_data

    def get_price_chunk(self, start_date, end_date):
        tmp = self.price_data[self.price_data.index.get_level_values('calendardate') < pd.Timestamp(end_date)]
        if not self.all_prev:
            tmp = self.price_data[tmp.index.get_level_values('calendardate') > pd.Timestamp(start_date)]
        return tmp

    def get_fund_chunk(self, start_date, end_date):
        tmp = self.price_data[self.fund_data.index.get_level_values('calendardate') < pd.Timestamp(end_date)]
        if not self.all_prev:
            tmp = self.fund_data[tmp.index.get_level_values('calendardate') > pd.Timestamp(start_date)]
        return tmp

    def get_asset_price(self, date, ticker):
        return self.price_data.loc[ticker].loc[date]['high']




