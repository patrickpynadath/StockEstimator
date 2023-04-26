from Utils import get_trading_dates, split_time_series
import datetime as dt

# if lookback_period = None, presented with all past data
class Chunker:
    def __init__(self,
                 start_date,
                 end_date,
                 chunk_size):
        self.start_date = start_date
        self.end_date = end_date
        self.chunk_size = chunk_size
        self.trading_dates = get_trading_dates(start_date, end_date)
        self.chunks = split_time_series(self.trading_dates, chunk_size)

    def __getitem__(self, item):
        return self.chunks[item]

    def __len__(self):
        return len(self.chunks)


