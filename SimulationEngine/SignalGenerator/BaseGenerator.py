class BaseGenerator:
    def __init__(self, SEP_tickers, SFP_tickers, FundColumns):
        self.sep = SEP_tickers
        self.sfp = SFP_tickers
        self.fund_columns = FundColumns

    def generate_signal(self, data):
        return data
