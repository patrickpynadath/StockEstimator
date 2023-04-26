import numpy as np
import pandas as pd
import pywt as wt
from scipy.fft import fft


def get_market_corr(ticker_data, window_size=30):
    sr = ticker_data['smooth_returns']
    spy = sr.loc['SPY']
    tickers = list(ticker_data.index.get_level_values('ticker').unique())
    to_concat = []
    for t in tickers:
        if t != 'SPY':
            ret_t = sr.loc[t]
            rolling_corr = spy.rolling(window=window_size).corr(ret_t)
            date_idx = ret_t.index
            mux = pd.MultiIndex.from_product([[t], date_idx], names=['ticker', 'calendardate'])
            rolling_corr = pd.Series(rolling_corr.values, index=mux)
            to_concat.append(rolling_corr)
    corr_data = pd.concat(to_concat).dropna()
    return corr_data


def get_fft_data(signal, idx, max_freq = None):
    tmp = pd.DataFrame()
    if not max_freq:
        max_freq = len(signal) // 2
    out = fft(signal)[:max_freq]
    tmp['magnitude'] = pd.Series(np.abs(out), index=idx)
    tmp['angle'] =pd.Series(np.angle(out), index=idx)
    return tmp


def smooth_target_col(price_data, max_res_ratio=.9, target_col='returns', name='smooth_returns'):
    tickers = (price_data.index.get_level_values('ticker').unique())
    to_concat = []
    for t in tickers:
        idx = price_data.loc[t][target_col].index
        returns = price_data.loc[t][target_col].values
        mux = pd.MultiIndex.from_product([[t], idx], names=['ticker', 'calendardate'])
        sr = wavelet_smooth(returns, max_res_ratio=max_res_ratio)[:len(idx)]
        sr = pd.Series(sr, index=mux, name=name)
        to_concat.append(sr)
    price_data[name] = pd.concat(to_concat)
    return price_data


def wavelet_smooth(ts, max_res_ratio , wavelet='sym4', mode='symmetric'):
    coeffs = get_coeff(ts, wavelet)
    max_res = int(max_res_ratio * len(coeffs))
    for i in range(len(coeffs))[max_res:]:
        coeffs[i] = np.zeros_like(coeffs[i])
    rec = wt.waverec(coeffs, wavelet, mode=mode)
    return rec


def get_coeff(ts, wavelet='sym5', mode='symmetric'):
    return wt.wavedec(ts, wavelet, mode=mode)


def calc_pct_change_mux(dataframe, target_name, out_name):
    tickers = list(dataframe.index.get_level_values('ticker').unique())
    to_concat = []
    for t in tickers:
        t_daily_ret = dataframe.loc[t][target_name].pct_change(periods=1)
        idx = t_daily_ret.index
        mux = pd.MultiIndex.from_product([[t], idx], names=['ticker', 'calendardate'])
        to_concat.append(pd.Series(t_daily_ret.values, index=mux))
    dataframe['smooth_returns'] = pd.concat(to_concat)
    return dataframe