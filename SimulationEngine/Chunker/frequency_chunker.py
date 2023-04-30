import numpy as np
import pandas as pd
from sklearn.cluster import SpectralClustering
from sklearn.decomposition import PCA
from Utils import get_market_corr
from tqdm import tqdm
import datetime as dt

# for now, just going to return all the data for both fundamental and equity prices
from Utils import get_fft_data
from Utils import get_min_daterange, get_price_data, get_market_corr, smooth_target_col, calc_pct_change_mux

ETF_TICKERS = ['XLE', 'XLB', 'XLI', 'XLY', 'XLP', 'XLV', 'XLF', 'SMH', 'XTL', 'XLU', 'SPY']


# need to get all etf data to be from same start date and end date
def get_etf_chunks(ret_data,
                   corr_data,
                   chunk_size,
                   max_freq,
                   overlap_ratio=.75):
    chunked_data = []
    cur_start_idx = 0
    dates = get_min_daterange(corr_data)
    final_end_idx = len(dates)
    date_ranges = []
    while cur_start_idx + chunk_size < final_end_idx:
        cur_chunk_corr = []
        cur_chunk_ret = []
        date_range = dates[cur_start_idx: cur_start_idx + chunk_size]
        date_ranges.append(date_range)
        freq_idx = range(max_freq)
        for t in ETF_TICKERS:
            mux = pd.MultiIndex.from_product([[t], freq_idx])
            if t != 'SPY':
                corr_signal = corr_data.loc[t].loc[date_range].values
                tmp = get_fft_data(corr_signal, mux, max_freq=max_freq)
                cur_chunk_corr.append(tmp)
            ret_signal = ret_data.loc[t].loc[date_range].values
            tmp = get_fft_data(ret_signal, mux, max_freq=max_freq)
            cur_chunk_ret.append(tmp)
        cur_chunk_corr = pd.concat(cur_chunk_corr)
        cur_chunk_ret = pd.concat(cur_chunk_ret)
        cur_chunk = pd.concat({'CorrData':cur_chunk_corr, 'RetData':cur_chunk_ret}, axis=1)
        chunked_data.append(cur_chunk)
        cur_start_idx += int((1-overlap_ratio) * chunk_size)
    return date_ranges, chunked_data


def stack_across_tickers(table, tickers, col_to_stack):
    return np.hstack([table.loc[t].values[:, 0] for t in tickers])


def unchunk_data(chunks):
    etf_tickers = [t for t in ETF_TICKERS if t != 'SPY']
    corr_magn = []
    corr_angle = []
    ret_magn = []
    ret_angle = []
    for ch in chunks:
        corr_magn.append(stack_across_tickers(ch['CorrData'], etf_tickers, 0))
        corr_angle.append(stack_across_tickers(ch['CorrData'], etf_tickers, 1))
        ret_magn.append(stack_across_tickers(ch['RetData'], ETF_TICKERS, 0))
        ret_angle.append(stack_across_tickers(ch['RetData'], ETF_TICKERS, 1))
    corr_magn = np.stack(corr_magn, axis=0)
    corr_angle = np.stack(corr_angle, axis=0)
    ret_magn = np.stack(ret_magn, axis=0)
    ret_angle = np.stack(ret_angle, axis=0)
    return {'corr_magn' : corr_magn,
           'corr_angle':corr_angle,
           'ret_magn':ret_magn,
           'ret_angle':ret_angle}


def leveled_pca_chunks(chunks, n_components=2):
    res = unchunk_data(chunks)
    pca_vector = []
    for key in res.keys():
        X = res[key]
        X = (X - X.min())/(X.max() - X.min())
        pca = PCA(n_components=n_components)
        y = pca.fit_transform(X)
        pca_vector.append(y)
    return np.hstack(pca_vector)


def get_highest_freq(ticker_data, top_k=3):
    tmp = ticker_data.sort_values(by='magnitude', ascending=False)
    tmp = tmp.reset_index()

    tmp.rename(columns={'index': 'freq'}, inplace=True)
    return tmp[:top_k]


def fft_distance_measure(s1, s2):
    score = 0
    for i in range(len(s1)):
        freq_dist = np.abs(s1.iloc[i]['freq'] - np.abs(s2.iloc[i]['freq']))
        score += freq_dist
        if freq_dist == 0:
            # needs to be less than 1 '

            magn_dist = pyth_dist(s1.iloc[i]['magnitude'], s2.iloc[i]['magnitude'])
            angle_dist = pyth_dist(s1.iloc[i]['angle'], s2.iloc[i]['angle'])
            score += magn_dist + angle_dist
    return score


def pyth_dist(a, b):
    if a == 0 and b == 0:
        return 0
    return np.abs(a - b) / ((a ** 2 + b ** 2) ** .5)



# object for chunking data for backtesting
# gonna use spectral cluster + custom distance function -- think it'll be better
class FreqChunker:
    def __init__(self,
                 start_date,
                 end_date,
                 chunk_size,
                 max_frequency,
                 max_res_ratio=.75,
                 n_per_regime=10,
                 n_regime=10):
        # get the data needed for chunking
        self.start_date = start_date
        self.end_date = end_date
        self.chunk_size = chunk_size
        self.max_frequency = max_frequency
        self.max_res_ratio = max_res_ratio
        self.overlap_ratio = .5
        self.n_per_regime = n_per_regime
        self.n_regime = n_regime
        self.chunks = self.create_chunks()

    def create_chunks(self):
        date_ranges, res = create_chunked_data(self.start_date, self.end_date, self.max_res_ratio, self.chunk_size, self.overlap_ratio, self.max_frequency)
        chunked = chunk_frequency_data(res)
        unique_dist, dist_matrix = construct_distance_matrix(chunked)
        spec_cluster = SpectralClustering(affinity='precomputed_nearest_neighbors', n_clusters=self.n_regime)
        labels = spec_cluster.fit_predict(dist_matrix)
        grouped = pd.DataFrame(labels).reset_index().groupby(0)
        n_per_regime = self.n_per_regime
        sampled_chunks = grouped.apply(lambda x : x.sample(n= min(n_per_regime, len(x))))['index'].values
        return [date_ranges[i] for i in sampled_chunks]
        # how many chunks should I get?

    def __getitem__(self, item):
        return self.chunks[item]

    def __len__(self):
        return len(self.chunks)


def create_chunked_data(start_date, end_date, max_res_ratio, chunk_size, overlap_ratio, max_frequency):
    etf_table = get_price_data(tickers=ETF_TICKERS, start_date=start_date, end_date=end_date, table='SFP')
    etf_table = smooth_target_col(etf_table, max_res_ratio=max_res_ratio, target_col='low', name='smooth_prices')
    etf_table = calc_pct_change_mux(etf_table, 'smooth_prices', 'smooth_returns')
    corr_data = get_market_corr(etf_table)
    date_ranges, res = get_etf_chunks(etf_table['smooth_returns'], corr_data, chunk_size, max_frequency,
                                      overlap_ratio=overlap_ratio)
    return date_ranges, res


def chunk_frequency_data(res):
    freq_data = []
    for i, ch in enumerate(res):
        cur_chunk = []
        corr = ch['CorrData'].dropna()
        freq_corr = [get_highest_freq(corr.loc[t]) for t in ETF_TICKERS if t != 'SPY']
        freq_corr = pd.concat(freq_corr, ignore_index=True)

        ret = ch['RetData'].dropna()
        freq_ret = [get_highest_freq(ret.loc[t]) for t in ETF_TICKERS]
        freq_ret = pd.concat(freq_ret)
        cur_chunk = {'CorrData':freq_corr, 'RetData':freq_ret}
        freq_data.append(cur_chunk)
    return freq_data


def construct_distance_matrix(chunked_frequency_rankings):
    unique_dist = []
    total_length = len(chunked_frequency_rankings)
    dist_matrix = np.zeros((total_length, total_length))
    for i in range(total_length):
        for j in range(i, total_length):
            if i != j:
                if i < j:
                    corr_dist = fft_distance_measure(chunked_frequency_rankings[i]['CorrData'], chunked_frequency_rankings[j]['CorrData'])
                    ret_data = fft_distance_measure(chunked_frequency_rankings[i]['RetData'], chunked_frequency_rankings[j]['RetData'])
                    total_dist = corr_dist + ret_data
                    dist_matrix[i, j] = corr_dist
                    dist_matrix[j, i] = ret_data
                    unique_dist.append(total_dist)
    return unique_dist, dist_matrix


# def get_cur_freq_data(chunk_size, chunked_frequency_rankings):
#     end_date = dt.date.today()
#     start_date = end_date - dt.timedelta(days=chunk_size)
#     etf_table = get_price_data(tickers=ETF_TICKERS, start_date=start_date, end_date=end_date, table='SFP').iloc[-chunk_size//2:]
#     etf_table = smooth_target_col(etf_table, target_col='low', name='smooth_prices')
#     etf_table = calc_pct_change_mux(etf_table, 'smooth_prices', 'smooth_returns')
#     corr_data = get_market_corr(etf_table)
#     for t in ETF_TICKERS:









