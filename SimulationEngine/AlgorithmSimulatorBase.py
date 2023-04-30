from .Portfolio import BasicPortfolio
from .ActionAlgorithms import Actor
from .Chunker import Chunker, FreqChunker
from .DataPipeline import DataPipe
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
        self.start_date = start_date
        self.end_date = end_date
        self.actor = actor
        self.aggregate_performance = []
        if chunk_type == 'base':
            chunker = Chunker(start_date, end_date, chunk_size)
        else:
            chunker = FreqChunker(start_date, end_date, chunk_size, **kwargs)
        period_start = start_date - dt.timedelta(days=burn_in_dur)
        period_end = end_date
        if not data_save_name:
            data_save_name = dt.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        ticker_info = self.actor.get_needed_tickers()
        pipeline = DataPipe(period_start,
                            period_end,
                            ticker_info['sep'],
                            ticker_info['sfp'],
                            data_save_name,
                            all_prev=actor.need_all_prev,
                            signal_generators=signal_generators)
        self.pipeline = pipeline
        self.chunker = chunker
        self.chunk_lookback = chunk_lookback
        self.need_all_prev= actor.need_all_prev

    def run_simulation_loop(self, starting_cash):
        total_outcomes = []
        portfolio_lists = []
        for i, date_chunk in enumerate(self.chunker):
            print(f"{i}# Chunk")
            action_outcomes, portfolio = self.simulate_single_chunk(date_chunk, starting_cash)
            total_outcomes.append(action_outcomes)
            portfolio_lists.append(portfolio)
        return total_outcomes, portfolio_lists

    def simulate_single_chunk(self, date_index, starting_cash):
        cur_idx = 0
        cur_trading = date_index[cur_idx]
        portfolio = BasicPortfolio(starting_cash, cur_trading)
        action_outcomes = []
        while cur_idx < len(date_index):
            cur_trading = date_index[cur_idx]
            data_start = date_index[cur_idx] - dt.timedelta(days=35)
            fund_chunk = self.pipeline.get_fund_chunk(data_start, cur_trading)
            price_data = self.pipeline.get_price_chunk(data_start, cur_trading)
            signal_chunk = self.pipeline.get_signal_chunk(data_start, cur_trading)
            actions = self.actor.act_on_market(portfolio, fund_chunk, price_data, signal_chunk, cur_trading)
            for act in actions['BUY']:
                ticker = act['ticker']
                price = self.pipeline.get_asset_price(cur_trading, ticker)
                quantity = act['quantity']
                res = portfolio.buy_asset(ticker, quantity, price, cur_trading, table=act['table'])
                action_outcomes.append(res)
            sell_actions = []
            for act in actions['SELL']:
                ticker = act['ticker']
                price = self.pipeline.get_asset_price(cur_trading, ticker)
                quantity = act['quantity']
                act['price'] = price
                act['date'] = cur_trading
                sell_actions.append(act)
            res = portfolio.sell_assets(sell_actions)
            action_outcomes.append(res)
            portfolio.update_historical(cur_trading)
            cur_idx += 1
        return action_outcomes, portfolio

    def get_agg_data(self, hist_pfts):
        weights = []
        values = []
        price_data = self.pipeline.get_price_chunk(self.start_date, self.end_date)
        for p in hist_pfts:
            w, v = p.get_historical_values(price_data)
            weights.append(w)
            values.append(v)
        return weights, values

    def get_chunks(self):
        return self.chunker.chunks

    def get_asset_history(self, hist_pfts):
        return [p.asset_hist for p in hist_pfts]



