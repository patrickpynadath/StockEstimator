from collections import namedtuple
import pandas as pd

# creating named tuples for storing information

transaction = namedtuple("transaction", "calendar_date action ticker quantity shareprice priorcb postcb table")
owned_asset = namedtuple('owned_asset', "ticker quantity purchasedate purchaseprice table")
individual_asset = namedtuple('ind_asset', 'ticker purchasedate saledate purchaseprice saleprice')


class BasicPortfolio:
    def __init__(self, cash_balance, start_date):
        # elements for trans history: ['CalendarDate', 'Action', 'Asset', 'Quantity', 'SharePrice', 'PriorCB', 'PostCB']
        #
        self.historical_records = []
        self.cur_assets = []
        self.hist_assets = {}
        self.asset_hist = []
        self.transaction_history = []
        self.cash_balance = cash_balance

    def buy_asset(self, ticker, quantity, price, cur_date, table='SEP'):
        if quantity * price > self.cash_balance:
            return -1

        t = transaction(cur_date, 'BUY', ticker, quantity, price, self.cash_balance, self.cash_balance - price * quantity, table)

        self.transaction_history.append(t)
        new_asset = owned_asset(ticker, quantity, cur_date, quantity * price, table)
        self.cur_assets.append(new_asset)

        self.cash_balance -= quantity * price

        return 0

# need to rework selling mechanism
    def sell_assets(self, sell_actions):
        to_remove = []
        for act in sell_actions:
            # info needed in each item: ticker, quantity, price, cur_date, pos_idx, table='SEP'
            pos_idx = act['pos_idx']
            quantity = act['quantity']
            cur_date = act['date']
            ticker = act['ticker']
            table = act['table']
            price = act['price']
            matching_asset = self.cur_assets[pos_idx]
            to_remove.append(pos_idx)
            if matching_asset.quantity - quantity > 0:
                new_asset = owned_asset(matching_asset.ticker, matching_asset.quantity - quantity, matching_asset.purchasedate,
                                        matching_asset.purchaseprice, matching_asset.table)
                self.cur_assets.append(new_asset)
            elif matching_asset.quantity - quantity == 0:
                pass
            else:
                return -1 # own negative shares, issue with algorithm
            trans = transaction(cur_date, 'SELL', ticker, quantity, price, self.cash_balance, self.cash_balance + quantity * price, table)
            self.cash_balance += price * quantity
            self.transaction_history.append(trans)
            for _ in range(quantity):
                self.asset_hist.append(individual_asset(ticker, matching_asset.purchasedate, cur_date,
                                                        matching_asset.purchaseprice, price))
        new_cur_assets = []
        for i, asset in enumerate(self.cur_assets):
            if i in to_remove:
                pass
            else:
                new_cur_assets.append(asset)
        self.cur_assets = new_cur_assets
        return 0

    def update_historical(self, cur_date):
        cash_asset = owned_asset('cash', self.cash_balance, cur_date, self.cash_balance, 'NA')
        tmp = self.cur_assets.copy()
        tmp.append(cash_asset)
        self.hist_assets[cur_date] = tmp
        return

    def get_tickers(self):
        tickers = []
        for a in self.cur_assets:
            if a.ticker not in tickers:
                tickers.append(a.ticker)
        return tickers

    def get_historical_values(self, price_data):
        # Info needed: date, cur_val, purchase_val
        values = []
        weights = []
        dates = []
        for date in self.hist_assets.keys():
            dates.append(date)
            present_values = {}
            ticker_weights = {}

            portfolio = self.hist_assets[date]
            total_value = 0
            for asset in portfolio:
                if asset.ticker != 'cash':
                    pv = price_data.loc[asset.ticker].loc[date]['high'] * asset.quantity
                else:
                    pv = asset.quantity
                total_value += pv
                if asset.ticker in present_values.keys():
                    present_values[asset.ticker] += pv
                else:
                    present_values[asset.ticker] = pv
                if asset.ticker in ticker_weights.keys():
                    ticker_weights[asset.ticker] += pv
                else:
                    ticker_weights[asset.ticker] = pv

            for t in ticker_weights.keys():
                ticker_weights[t] /= total_value
            weights.append(ticker_weights)
            values.append(present_values)
        values = pd.DataFrame(values, index=dates)
        total = values.fillna(value=0).sum(axis=1)
        values['Total'] = total
        weights = pd.DataFrame(weights, index=dates)
        return weights, values

