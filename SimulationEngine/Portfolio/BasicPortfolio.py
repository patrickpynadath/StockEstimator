import pandas as pd
class BasicPortfolio:
    def __init__(self, cash_balance, start_date):

        self.historical_records = []
        self.cur_assets = pd.DataFrame(columns=['AssetTicker', 'Quantity', 'PurchaseDate','PurchasePrice'])
        self.cur_assets.set_index(['AssetTicker', 'PurchaseDate'])
        self.hist_assets = pd.DataFrame(columns=['AssetTicker', 'Quantity', 'PurchaseDate','PurchasePrice', 'SellDate', 'SellPrice', 'CalendarDate'])
        self.hist_assets = {}
        self.transaction_history = pd.DataFrame(columns=['CalendarDate', 'Action', 'Asset', 'Quantity', 'SharePrice', 'PriorCB', 'PostCB'])
        self.cash_balance = cash_balance

    def buy_asset(self, ticker, quantity, price, cur_date):
        if quantity * price > self.cash_balance:
            return -1

        self.transaction_history.append({'CalendarDate': cur_date,
                                         'Action': 'BUY',
                                         'Asset': ticker,
                                         'Quantity': quantity,
                                         'SharePrice': price,
                                         'PriorCB': self.cash_balance,
                                         'PostCB': self.cash_balance - price * quantity})

        self.cur_assets.append({'AssetTicker' : ticker,
                                'Quantity':quantity,
                                'PurchaseDate':cur_date,
                                'PurchasePrice': quantity * price})
        self.cash_balance -= quantity * price

        return 0

    def sell_asset(self, ticker, quantity, price, cur_date):
        if ticker in self.cur_assets:
            if self.cur_assets.loc[ticker]['Quantity'] <= quantity:
                self.cur_assets.drop(labels=[ticker], axis=0)
                self.transaction_history.append({'CalendarDate': cur_date,
                                                 'Action': 'SELL',
                                                 'Asset': ticker,
                                                 'Quantity': quantity,
                                                 'SharePrice': price,
                                                 'PriorCB': self.cash_balance,
                                                 'PostCB': self.cash_balance + price * quantity})
                self.cash_balance += price * quantity
                return 0
        return -1

    def update_historical(self, cur_date):
        self.hist_assets[cur_date] = self.cur_assets.copy()
        return

    def get_current_assets(self):
        return list(self.cur_assets.index.get_level_values('ticker').unique())

    # what values am I interested in?
    def get_historical_variance(self):
        return

    def get_historical_returns(self):
        return

    def get_historical_correlations(self):
        return

