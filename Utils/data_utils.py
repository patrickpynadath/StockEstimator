import nasdaqdatalink as nasdaq
import pandas as pd


def get_min_daterange(price_data):
    tickers = list(price_data.index.get_level_values('ticker').unique())
    t_to_use = tickers[0]
    start_date = price_data.loc[t_to_use].index[0]
    end_date = price_data.loc[t_to_use].index[-1]
    for t in tickers[1:]:
        cur_start = price_data.loc[t].index[0]
        cur_end = price_data.loc[t].index[-1]
        if cur_start > start_date:
            t_to_use = t
        if end_date <= cur_end:
            t_to_use = t
    return price_data.loc[t_to_use].index


def get_date_logic(start_date, end_date):
    return {'gte': start_date.strftime('%Y-%m-%d'),
                                           'lte': end_date.strftime('%Y-%m-%d')}


def get_trading_dates(start_date, end_date):
    dates = nasdaq.get_table(f"SHARADAR/SEP", ticker=['AAPL'],
                     date=get_date_logic(start_date, end_date),
                     paginate=True)['date'].sort_values(ascending=True)
    return dates


def get_price_data(tickers, start_date, end_date, table='SEP'):
    table_prices = nasdaq.get_table(f"SHARADAR/{table}", ticker=tickers,
                                    date=get_date_logic(start_date, end_date),
                                    paginate=True)
    table_prices.rename(columns={'date': 'calendardate'}, inplace=True)
    table_prices.set_index(['ticker', 'calendardate'], inplace=True)
    table_prices.sort_index(axis=0, ascending=True, inplace=True)
    return table_prices


def get_fundamental_data(dates, tickers):
    start_date = dates[0]
    end_date = dates[-1]
    table = nasdaq.get_table("SHARADAR/SF1", ticker=tickers,
                             calendardate=get_date_logic(start_date, end_date),
                             dimension=['ARQ'],
                             paginate=True)
    table.set_index(['ticker', 'calendardate'], inplace=True)
    table.sort_index(axis=0, ascending=True, inplace=True)
    mux = pd.MultiIndex.from_product([tickers, dates], names=['ticker', 'calendardate'])
    table = table.reindex(mux).fillna(method='ffill')
    return table


def split_time_series(date_index, chunk_size):
    chunks = []
    start_date = date_index[0]
    total_length = len(date_index)
    start_idx = 0
    while start_idx + chunk_size < total_length:
        chunks.append(date_index[start_idx:start_date + chunk_size])
    return chunks
