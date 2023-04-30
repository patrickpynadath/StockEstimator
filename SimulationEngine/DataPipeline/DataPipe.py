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
        to_concat = []
        if sep_tickers != []:
            sep_price_data = get_price_data(sep_tickers, period_start, period_end, table='SEP')
            to_concat.append(sep_price_data)
        if sfp_tickers != []:
            sfp_price_data = get_price_data(sfp_tickers, period_start, period_end, table='SFP')
            to_concat.append(sfp_price_data)
        if len(to_concat) == 1:
            price_data = to_concat[0]
        else:
            price_data = pd.concat(to_concat)
        dates = price_data.index.get_level_values('calendardate').unique()
        fund_data = get_fundamental_data(dates, sep_tickers)
        self.all_prev = all_prev
        os.mkdir(data_save_name)

        total_signal = []
        for sg in signal_generators:
            signal = sg.generate(price_data, fund_data)
            signal_name = sg.name
            total_signal = signal
        #total_signal = pd.concat([total_signal])

        total_signal.to_csv(f'{data_save_name}/signal.csv')
        fund_data.to_csv(f'{data_save_name}/fund_data.csv')
        price_data.to_csv(f'{data_save_name}/price_data.csv')
        self.fund_data = fund_data
        self.price_data = price_data
        self.signal_data = total_signal

    def get_price_chunk(self, start_date, end_date):
        tmp = self.price_data[self.price_data.index.get_level_values('calendardate') <= pd.Timestamp(end_date)]
        if not self.all_prev:
            tmp = tmp[tmp.index.get_level_values('calendardate') >= pd.Timestamp(start_date)]
        return tmp

    def get_fund_chunk(self, start_date, end_date):
        tmp = self.fund_data[self.fund_data.index.get_level_values('calendardate') <= pd.Timestamp(end_date)]
        if not self.all_prev:
            tmp = tmp[tmp.index.get_level_values('calendardate') >= pd.Timestamp(start_date)]
        return tmp

    def get_signal_chunk(self, start_date, end_date):
        tmp = self.signal_data[self.signal_data.index.get_level_values('calendardate') <= pd.Timestamp(end_date)]
        if not self.all_prev:
            tmp = tmp[tmp.index.get_level_values('calendardate') >= pd.Timestamp(start_date)]
        return tmp

    def get_asset_price(self, date, ticker):
        return self.price_data.loc[ticker].loc[date]['high']








