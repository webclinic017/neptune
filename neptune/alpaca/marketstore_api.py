#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module for performing various tasks involving the MarketStore database used to store and retrieve price data.
"""

# System
import logging
import re
from typing import List

import yaml
import numpy as np
import pandas as pd

# Third Party
import alpaca_trade_api
import pymarketstore

# Autotrader
from neptune.config import CONF_DIRECTORY, configure_logging
from rest_api import AlpacaRestAPI

class MarketStoreApi(pymarketstore.Client):
    # MarketStore database schema. Used to pack numpy arrays for database
    # TODO Cannot currently handle VWAP
    DATA_TYPES = np.dtype([('Epoch', 'i8'), ('Open', 'f4'), ('High', 'f4'), ('Low', 'f4'), ('Close', 'f4'), ('Volume', 'f4')])

    # Number of bars to retain for each Symbol/Timeframe combination
    HISTORY_LENGTH = 300

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.config = MarketStoreConfiguration()  # Using default mkts.yml

    def insert(self, bar: dict):
        """
        Insert bar data received from Alpaca streaming API into database.
        Convert Alpaca streaming API bar dict to numpy ndarray format for Marketstore.
        :param bar: dict of bar data from Alpaca websocket API
        """
        # Convert bar dict to numpy array with type data
        bar_time = bar['t'].seconds
        bar_tuple = (bar_time, bar['o'], bar['h'], bar['l'], bar['c'], bar['v'])
        payload = np.array([bar_tuple], dtype=self.DATA_TYPES)
        self.write(payload, '{}/1Min/OHLCV'.format(bar['S']))

    def populate(self, symbols: list):
        """
        Pre-populate data for given symbols in MarketStore database.
        :param symbols: list of symbols to populate
        """
        rest_api = AlpacaRestAPI()
        for tf in self.config.get_timeframes():
            timeframe = convert_timeframe(tf)
            start_date = determine_start_date(tf, self.HISTORY_LENGTH)
            for symbol in symbols:
                bars = rest_api.get_bars(symbol=symbol, timeframe=timeframe, start=start_date, limit=10000, adjustment='raw')
                bars_df = bars.df.tail(self.HISTORY_LENGTH).copy()
                print("Sym: {} | TF: {} | N: {}".format(symbol, tf, len(bars.df)))
                payload = convert_dataframe(bars_df)
                result = self.write(payload, '{}/{}/OHLCV'.format(symbol, tf))
                if result['responses'] is not None:
                    self.logger.error("Could not write {} {} data: {}".format(symbol, tf, result['responses']))

    def trim_all(self):
        """
        Trim all data in MarketStore to contain a maximum of HISTORY_LENGTH bars.
        """
        param_d = {k: self.HISTORY_LENGTH for k in self.config.get_timeframes()}
        self.trim(param_d)

    def trim(self, param_d: dict):
        """Trim all data according to Timeframe: Limit input dict.
        For example, to trim 1Min data to the last 2000 bars, params = {'1Min': 2000}
        :param param_d: data trimming specification
        """
        for timeframe, N in param_d.items():
            for symbol in self.list_symbols():
                logging.info("Trimming data for {}".format(symbol))
                search_params = pymarketstore.Params(symbols=symbol, timeframe=timeframe, attrgroup='OHLCV')
                reply = self.query(search_params).first()
                if reply.array.size > N:
                    result = self.destroy(reply.key)
                    result = self.write(reply.array[-N:], reply.key, isvariablelength=True)

    def query_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """
        Query data from MarketStore database.
        :param symbol: symbol to query
        :param timeframe: timeframe string (1Min, 2H, etc.)
        :return: pandas DataFrame containing bar data
        """
        params = pymarketstore.Params(symbols=symbol, timeframe=timeframe, attrgroup='OHLCV')
        reply = super().query(params).first().df()
        return reply

    def remove_data(self, symbol: str, timeframe: str):
        """
        Remove data from MarketStore database.
        :param symbol: symbol to query
        :param timeframe: timeframe string (1Min, 2H, etc.), "*" to remove all data
        """
        if timeframe == "*":
            timeframe_l = self.config.get_timeframes()
        else:
            timeframe_l = [timeframe]

        for tf in timeframe_l:
            table_str = "{}/{}/OHLCV".format(symbol, tf)
            try:
                self.logger.warning("Removing {}".format(table_str))
                self.destroy(table_str)
            except Exception as e:
                self.logger.error("Error removing {}: {}".format(table_str, e))

    def clear_database(self):
        """
        Clear all data from database. Requires user CLI confirmation.
        """
        confirm = input("Remove all items from MarketStore database. Type \"YES\" to continue")
        if confirm == "YES":
            logging.warning("****** REMOVING ALL DATA ******")
            symbols = self.list_symbols()
            for i, sym in enumerate(symbols):
                logging.warning("{}/{} | Removing {}".format(i + 1, len(symbols), sym))
                for tf in self.config.get_timeframes():
                    self.remove_data(sym, tf)


class MarketStoreConfiguration:
    DEFAULT_TIMEFRAMES = ['1Min', '3Min', '5Min', '15Min', '30Min', '1H', '2H', '1D']

    def __init__(self, mkts_yml: str = None):
        """
        Constructor for MarketStore configuration object
        :param mkts_yml: optional path mkts.yml used for MarketStore container configuration
        """

        # Use default mkts.yml if not provided
        if mkts_yml is None:
            mkts_yml = CONF_DIRECTORY / 'mkts.yml'

        # Read configuration data
        self.marketstore_config = {}
        with open(str(mkts_yml), 'r') as fp:
            self.marketstore_config = yaml.safe_load(fp)

    def get_timeframes(self) -> List[str]:
        """
        Retrieve timeframes from MarketStore configuration, otherwise, return the default list
        :return: timeframes from mkts.yml, if present, otherwise, default values
        """
        triggers = self.marketstore_config.get('triggers', None)
        if triggers is not None:
            ondiskagg_config = next(t for t in triggers if t['module'].find('ondiskagg') == 0)
            timeframes = ondiskagg_config.get('config', {}).get('destinations', self.DEFAULT_TIMEFRAMES)
        else:
            timeframes = self.DEFAULT_TIMEFRAMES

        # We should always have 1Min, but it won't be in the mkts.yml configuration
        # Remove duplicates and return
        timeframes.append('1Min')
        return list(set(timeframes))


def convert_timeframe(timeframe: str) -> alpaca_trade_api.TimeFrame:
    """
    Resolve string format bar width into Alpaca TimeFrame type.
    :param timeframe: input string timeframe (i.e. 1D, 2H, 15Min)
    :return: Alpaca API format TimeFrame type
    """
    match = re.compile("[^\W\d]").search(timeframe)
    number = int(timeframe[:match.start()])
    unit = timeframe[match.start():]

    if unit[0].upper() == "D":
        return alpaca_trade_api.TimeFrame(number, alpaca_trade_api.TimeFrameUnit.Day)
    elif unit[0].upper() == "H":
        return alpaca_trade_api.TimeFrame(number, alpaca_trade_api.TimeFrameUnit.Hour)
    elif unit[0].upper() == "M":
        return alpaca_trade_api.TimeFrame(number, alpaca_trade_api.TimeFrameUnit.Minute)
    else:
        raise ValueError("Timeframe unit not valid: {}".format(unit))


def determine_start_date(timeframe: str, n_bars: int) -> str:
    """
    Estimate a reasonable start date to pass Alpaca REST API for historical bars.
    :param timeframe: timeframe in string format (1Min, 15Min, 2H, etc.)
    :param n_bars: minimum number of bars we want from the API
    """
    now = pd.Timestamp.now()
    timedelta = pd.Timedelta(timeframe)
    if timedelta < pd.Timedelta('30Min'):
        k_mult = 10
    elif timedelta < pd.Timedelta('1D'):
        k_mult = 7
    else:
        k_mult = 1.5
    start_date = now - pd.Timedelta('2D') - n_bars * k_mult * timedelta
    return start_date.strftime('%Y-%m-%d')


def convert_dataframe(df: pd.DataFrame) -> np.array:
    """
    Convert Alpaca REST API returned DataFrame to numpy ndarray format for MarketStore.
    :param df: pandas DataFrame returned from Alpaca REST API
    :return: reshaped ndarray of tuples per MarketStore API
    """
    df.columns = df.columns.str.lower()
    df['epoch'] = df.index.view('int64') // 1e9
    df.reset_index(inplace=True)
    df = df[['epoch', 'open', 'high', 'low', 'close', 'volume']]
    return np.array([tuple(v) for v in df.values.tolist()], dtype=MarketStoreApi.DATA_TYPES)


if __name__ == '__main__':

    symbol_list = ['NVDA', 'BAC', 'MRK']

    # Typical API usage
    configure_logging()  # This is a package level construct that would be called in a main-like program

    # Create the API
    api = MarketStoreApi()

    # Populate some data, bar types take from mkts.yml file
    api.populate(symbol_list)

    # Query data for a symbol
    df = api.query_data('NVDA', '15Min')

    # Remove data for a given symbol, * will remove all bar widths for symbol
    api.remove_data('BAC', '*')

    # Trim number of rows for a given bar width (all symbols)
    api.trim({'15Min': 250})

    # Verify reduction of bars for 15Min data
    df2 = api.query_data('NVDA', '15Min')
    print("N bars: {}".format(len(df2)))

    api.clear_database()
    print()