import datetime

import streamlit as st
from StreamlitApp import setup_config_sidebar
from SimulationEngine import AlgSimulatorBase, Chunker, FreqChunker, BasicPortfolio, DataPipe
import pandas as  pd
import plotly.express as px


def get_overall_returns(values):
    r = []
    for v in values:
        overall_sum = v.sum(axis=1)
        ret = (overall_sum.iloc[-1] - overall_sum.iloc[0])/overall_sum.iloc[0]
        r.append(ret)
    return r


def get_overall_var(values):
    r = []
    for v in values:
        overall_sum = v.sum(axis=1)
        var = overall_sum.var()
        r.append(var)
    return r


def get_chunk_labels(chunks):
    labels = []
    for ch in chunks:
        d1 = ch[0].strftime('%D')
        d2 = ch[-1].strftime('%D')
        l = f'{d1} to {d2}'
        labels.append(l)
    return labels

@st.cache_resource
def get_simulator_obj(_res, cash):
    start_date = _res['start_date']
    end_date = _res['end_date']
    max_freq = _res['max_freq']
    actor = _res['actor']
    signal = _res['signal']
    chunk_lookback = _res['chunk_lookback']
    chunk_size = _res['chunk_size']
    burnin = _res['burnin']
    chunker = _res['chunker']
    simulator = AlgSimulatorBase(actor, start_date, end_date, burnin, chunker, chunk_size,
                                 signal_generators=[signal], chunk_lookback=chunk_lookback, max_frequency=max_freq)
    return simulator




ready_to_run, timestamp, res = setup_config_sidebar()
if ready_to_run:
    st.write("Enter Beginning Cash")
    begin_val = int(st.slider("Beginning Portfolio Cash", 1000, 1000000, 100000))
    simulate = st.button("Press to simulate")
    if simulate:
        ts = datetime.datetime.now()
        if 'last_press' not in st.session_state.keys():
            st.session_state['last_press'] = ts
        if 'sim' not in st.session_state.keys():
            simulator = get_simulator_obj(res, begin_val)
            _, hist_pfts = simulator.run_simulation_loop(begin_val)
            st.session_state['sim'] = simulator
            st.session_state['hist_pfts'] = hist_pfts
    if 'sim' in st.session_state.keys():
        simulator = st.session_state['sim']
        hist_pfts = st.session_state['hist_pfts']
        weights, values = simulator.get_agg_data(hist_pfts)
        weights = [w.fillna(value=0) for w in weights]
        values = [v.fillna(value=0) for v in values]
        st.title("Aggregate Data")
        tmp = pd.DataFrame()
        tmp['Returns'] = get_overall_returns(values)
        tmp["Stdev"] = get_overall_var(values)
        tmp['Stdev'] = tmp['Stdev'] ** .5
        # get best, worst, riskiest, safest from here
        best_idx = tmp['Returns'].sort_values(axis=0, ascending=False).iloc[0]
        worst_idx = tmp['Returns'].sort_values(axis=0, ascending=True).iloc[0]
        risky_idx = tmp['Stdev'].sort_values(axis=0, ascending=False).iloc[0]
        safe_idx = tmp['Stdev'].sort_values(axis=0, ascending=True).iloc[0]
        st.write(f"Mean Returns: {tmp['Returns'].mean()}")
        st.write(f"Mean Stdev: {tmp['Stdev'].mean()}")
        st.write(px.histogram(tmp, x='Returns', title="Agg Returns"))
        st.write(px.histogram(tmp, x='Stdev', title='Agg Stdev'))

        # st.title("Forward Looking")
        # # plot signal here
        st.title("Individual Chunk Data")
        chunks = simulator.get_chunks()
        labels = get_chunk_labels(chunks)
        st.write(f"Best Performing Chunk: {labels[best_idx]}")
        st.write(f"Worst Performing Chunk: {labels[worst_idx]}")
        st.write(f"Riskiest Chunk: {labels[risky_idx]}")
        st.write(f"Safest Chunk: {labels[safe_idx]}")
        selected_range = st.selectbox("Select Range", options=labels)
        idx_range = labels.index(selected_range)
        st.write(px.line(values[idx_range], title='Asset Values'))
        st.write(px.line(weights[idx_range], title='Asset Weights'))
        # need to make functions to turn data into dataframes so i can cache them


