from SimulationEngine import Actor
from SimulationEngine import Generator
import pandas as pd


class MeanRevActor(Actor):
    def __init__(self, action_value:float = 2):
        self.action_value = action_value
        super().__init__(sfp_tickers=[], name='MEAN_REV', sep_tickers=['AAPL', 'MSFT', 'AMZN'], need_all_prev=False)
        pass

    def act_on_market(self, cur_portfolio, chunk_fd, chunk_pd, chunk_signal, date):
        # check if need to buy
        BUY= []
        SELL = []
        # check if need to sell
        cur_cash = cur_portfolio.cash_balance
        for ticker in self.sep_tickers:
            if chunk_pd.loc[ticker]['high'].loc[date] < cur_cash:
                if chunk_signal[f'{ticker}_signal'].loc[date] < -1 * self.action_value:
                    cur_cash -= chunk_pd.loc[ticker]['high'].loc[date]
                    BUY.append({'ticker':ticker, 'quantity':1, 'table':'SEP'})
        for i, asset in enumerate(cur_portfolio.cur_assets):
            ticker = asset.ticker
            if chunk_signal[f'{ticker}_signal'].loc[date] > 2 * self.action_value:
                cur_cash += chunk_pd.loc[ticker]['high'].loc[date]
                SELL.append({'ticker': ticker, 'quantity': asset.quantity, 'table':'SEP', 'pos_idx':i})
            if chunk_signal[f'{ticker}_signal'].loc[date] < -2 * self.action_value:
                cur_cash += chunk_pd.loc[ticker]['high'].loc[date]
                SELL.append({'ticker': ticker, 'quantity': asset.quantity, 'table':'SEP', 'pos_idx':i})

        return {'BUY':BUY, 'SELL':SELL}


class MeanReversalSignal(Generator):
    def __init__(self, window_size: int, error_margin: float):
        super().__init__(SEP_tickers=['AAPL', 'MSFT', 'AMZN'], SFP_tickers=[], FundColumns=[])
        self.window_size = window_size
        self.error_margin =error_margin
        self.name = "mean_reversal"

    def generate(self, price_data, fund_data):
        df = pd.DataFrame()
        for ticker in ['MSFT', 'AMZN', 'AAPL']:
            # idea: if asset is error margin above
            stdev = price_data.loc[ticker]['low'].rolling(window=self.window_size*2).std()
            mean1 = price_data.loc[ticker]['low'].rolling(window=self.window_size).mean()
            mean2 = price_data.loc[ticker]['low'].rolling(window=self.window_size * 2).mean()

            df[f'{ticker}_signal'] = (mean1 - mean2)/(stdev * self.error_margin)
        return df
