import streamlit as st
from os import listdir
from os.path import isfile, join
import datetime as dt
import importlib
import inspect


def get_dir_files(path):
    mypath = path
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    return onlyfiles


def get_actor_parameters(actor_class):
    sig = inspect.signature(actor_class)
    l = list(sig.parameters.keys())
    st.write("Select parameters for Actor Class")
    param_values = {}
    for param_name in l:
        t = sig.parameters[param_name].annotation
        param_values[param_name] = (st.text_input(f"Param Val for {param_name}, type {t}"), t)
    return param_values


def get_signal_parameters(sig_class):
    sig = inspect.signature(sig_class)
    l = list(sig.parameters.keys())
    st.write("Select parameters for Signal Class")
    param_values = {}
    for param_name in l:
        t = sig.parameters[param_name].annotation
        param_values[param_name] = (st.text_input(f"Param Val for {param_name}, type {t}"), t)
    return param_values


def get_actor_class(module):
    classes = inspect.getmembers(module, inspect.isclass)
    options = [c[0] for c in classes]
    opt = st.selectbox("Select Actor class", options)
    return classes[options.index(opt)][1]


def get_signal_class(module):
    classes = inspect.getmembers(module, inspect.isclass)
    options = [c[0] for c in classes]
    opt = st.selectbox("Select Signal class", options)
    return classes[options.index(opt)][1]


def get_date_range():
    start = st.date_input(label='Start Date for Backtest', value=dt.date(day=1, year=2013, month=1))
    end = st.date_input(label='End Date for Backtest', value=dt.date(day=1, year=2023, month=1))
    if end <= start:
        st.markdown("**:red[Enter Valid Date]**")
    return start, end


def get_chunker_type():
    chunker_type = st.selectbox("Chunking Algorithm to Use", options=['base', 'freq'])
    return chunker_type


def get_max_freq(chunk_size):
    max_freq = st.slider("Max Frequency to record", int(chunk_size /4), chunk_size, chunk_size // 2)
    return max_freq


def get_burnin_dur():
    burnin = st.slider("Alg Burnin", 0, 300, 150)
    return burnin


def convert_to_args(dct):
    new_dct = {}
    for k in dct.keys():
        v = dct[k][0]
        t = dct[k][1]
        new_dct[k] = t(v)
    return new_dct


def get_chunk_size():
    chunk_size = st.slider("Number of Points for each chunk", 30, 100, 60)
    return chunk_size


def get_chunk_lookback(chunk_size):
    chunk_lookback = st.slider("Number of Points to Look Backwards for Algorithm", chunk_size // 4, chunk_size)
    return chunk_lookback


def setup_config_sidebar(path='AlgoDevelopment'):
    with st.sidebar:
        file_list = get_dir_files(path)
        file = st.selectbox(label='Algorithm File To Run', options=file_list)

        do_import = st.selectbox("Ready to Import?", options=("No", "Yes"), index=0)
        if do_import == "Yes":
            target_module = importlib.import_module(f"{path}.{file[:-3]}")
            actor = get_actor_class(target_module)
            signal = get_signal_class(target_module)
            do_param_config = st.selectbox("Ready to Config?", options=("No", "Yes"), index=0)
            if do_param_config == "Yes":
                actor_params = get_actor_parameters(actor)
                signal_params = get_signal_parameters(signal)

        start, end = get_date_range()
        chunker = get_chunker_type()
        burnin = get_burnin_dur()
        chunk_size = int(get_chunk_size())
        if chunker == 'freq':
            max_freq = int(get_max_freq(chunk_size))
        else:
            max_freq = 0
        lookback = get_chunk_lookback(chunk_size)
        to_return = st.selectbox("Ready to run?", options=("No", "Yes"), index=0)
        if to_return == "Yes":
            actor_params = convert_to_args(actor_params)
            signal_params = convert_to_args(signal_params)
            act = actor(**actor_params)
            sig = signal(**signal_params)
            res = {"actor" : act,
                   "signal":sig,
                   "max_freq":max_freq,
                   "chunk_lookback":lookback,
                   "chunk_size":chunk_size,
                   "start_date":start,
                   "end_date":end,
                   "burnin" : burnin,
                   "chunker":chunker}
            return (True, dt.datetime.today(), res)
        else:
            return (False, 0, 0)


