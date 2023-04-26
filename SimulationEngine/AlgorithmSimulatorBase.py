from DataPipeline import DataPipe
from Chunker import Chunker, FreqChunker
from ActionAlgorithms import Actor
from Portfolio import BasicPortfolio
import datetime as dt

# want to also run a trading simulator where models can be trained
class AlgSimulatorBase:
    def __init__(self,
                 actor: Actor,
                 start_date,
                 end_date,
                 burn_in_dur,
                 chunk_type,
                 chunk_size,
                 data_save_name=False,
                 signal_generators = [],
                 chunk_lookback=0, **kwargs):
        self.actor = actor
        self.aggregate_performance = []
        if chunk_type == 'base':
            chunker = Chunker(start_date, end_date, chunk_size)
        else:
            chunker = FreqChunker(start_date, end_date, chunk_size, **kwargs)
        period_start = start_date - dt.timedelta(days=burn_in_dur)
        period_end = end_date
        if not data_save_name:
            data_save_name = dt.datetime.now().strftime("%m_%d_%Y_%H_M_S")
        ticker_info = self.actor
        pipeline = DataPipe(period_start,
                            period_end,
                            ticker_info['sep'],
                            ticker_info['sfp'],
                            data_save_name,
                            all_prev=actor.need_all_prev)
        self.pipeline = pipeline
        self.chunker = chunker
        self.chunk_lookback = chunk_lookback
        self.need_all_prev= actor.need_all_prev

    def run_simulation_loop(self, starting_cash):
        total_outcomes = []
        portfolio_lists = []
        for i, date_chunk in self.chunker:
            action_outcomes, portfolio = self.simulate_single_chunk(date_chunk, starting_cash)
            total_outcomes.append(action_outcomes)
            portfolio_lists.append(portfolio)
        return total_outcomes, portfolio_lists

    def simulate_single_chunk(self, date_index, starting_cash):
        cur_idx = 0
        cur_trading = date_index[cur_idx]
        data_start = date_index[cur_idx] - dt.timedelta(days=35)
        portfolio = BasicPortfolio(starting_cash, cur_trading)
        action_outcomes = []
        while cur_idx < len(date_index):
            fund_chunk = self.pipeline.get_fund_chunk(data_start, cur_trading)
            price_data = self.pipeline.get_price_chunk(data_start, cur_trading)
            actions = self.actor.act_on_market(portfolio, fund_chunk, price_data)
            for act in actions['BUY']:
                ticker = act['ticker']
                price = self.pipeline.get_asset_price(cur_trading, ticker)
                quantity = act['quantity']
                res = portfolio.buy_asset(ticker, quantity, price, cur_trading)
                action_outcomes.append(res)
            for act in actions['SELL']:
                ticker = act['ticker']
                price = self.pipeline.get_asset_price(cur_trading, ticker)
                quantity = act['quantity']
                res = portfolio.sell_asset(ticker, quantity, price, cur_trading)
                action_outcomes.append(res)
            cur_idx += 1
            cur_trading = date_index[cur_idx]
            data_start = date_index[cur_idx] - dt.timedelta(days=35)
        return action_outcomes, portfolio

    def get_hist_port_perf(self, portfolio, chunk_idx):
        return

    def calculate_aggregate_performance(self, portfolio):
        return


