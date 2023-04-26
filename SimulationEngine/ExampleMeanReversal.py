from ActionAlgorithms import Actor
from SignalGenerator import BaseGenerator
import pandas as pd


class MeanRevActor(Actor):
    def __init__(self):
        super().__init__(sfp_tickers=[], sep_tickers=['AAPL', 'MSFT', 'AMZN'], need_all_prev=False)
        pass

    def act_on_market(self, cur_portfolio, chunk_fd, chunk_pd, chunk_signal, date):
        # check if need to buy
        BUY= []
        SELL = []
        # check if need to sell
        cur_cash = cur_portfolio.cash_balance
        for ticker in self.sep_tickers:
            if chunk_pd.loc[ticker]['high'].loc[date] < cur_cash:
                if chunk_signal[f'{ticker}_signal'].loc[date] > 0:
                    cur_cash -= chunk_pd.loc[ticker]['high']
                    BUY.append({'ticker':ticker, 'quantity':1})
        for ticker in cur_portfolio.get_tickers():
            if chunk_signal[f'{ticker}_signal'].loc[date] < 0:
                cur_cash += chunk_pd.loc[ticker]['high']
                SELL.append({'ticker': ticker, 'quantity': 1})

        return {'BUY':BUY, 'SELL':SELL}


class MeanReversalSignal(BaseGenerator):
    def __init__(self, window_size, error_margin):
        super().__init__()
        self.window_size = window_size
        self.error_margin =error_margin

    def generate_signal(self, price_data):
        df = pd.DataFrame()
        for ticker in ['MSFT', 'AMZN', 'AAPL']:
            # idea: if asset is error margin above
            stdev = price_data.loc[ticker]['low'].rolling(window=self.window_size*3).std()
            mean1 = price_data.loc[ticker]['smooth_price'].rolling(window=self.window_size).mean()
            mean2 = price_data.loc[ticker]['smooth_price'].rolling(window=self.window_size * 2).mean()

            df[f'{ticker}_signal'] = (mean1 - mean2)/(stdev * self.error_margin)
        return df
