# base class to be implemented
# later will add some cool stuff, this is just to finish everything else up
class Actor:
    def __init__(self, sfp_tickers, sep_tickers, need_all_prev=False):
        self.sep_tickers = sep_tickers
        self.sfp_tickers = sfp_tickers
        self.need_all_prev = need_all_prev
        pass

    # what functions would need to be implemented
    # return list of actions in order of ticker, quantity, price, cur_date
    def act_on_market(self, cur_portfolio, chunk_fd, chunk_pd, chunk_signal, date):
        buy_actions = []
        sell_actions = []
        return {'BUY': buy_actions, 'SELL': sell_actions}

    def get_needed_tickers(self):
        return {'sep':self.sep_tickers, 'sfp':self.sfp_tickers}
