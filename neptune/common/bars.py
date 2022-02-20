#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility module for handling of OHLCV bar data.
"""

from __future__ import annotations

from typing import List, Dict, Union
import pandas as pd
import numpy as np
import math
from neptune.alpaca import AlpacaRestAPI
from alpaca_trade_api import TimeFrame, TimeFrameUnit

import pandas_ta as pdta


class OHLCV:
    COLUMNS = ['open', 'high', 'low', 'close', 'volume', 'vw']

    def __init__(self, symbol: str, ohlcv_df: pd.DataFrame):
        """Create OHLCV object. Unless the columns are named correctly, the static factory methods defined
        below should be used for object construction.
        :param symbol: ticker string
        :param ohlcv_df: input DataFrame with column names correctly labeled (per COLUMNS above)
        """
        self.symbol = symbol
        self.df = ohlcv_df
        self.bar_duration = self.df.index.to_series().diff().min()

    @property
    def bar_width(self):
        """Return bar width of OHLCV data."""
        return self.df.index.to_series().diff().min()

    def resample(self, width: Union[str, pd.Timedelta], return_df=False) -> Union[OHLCV, pd.DataFrame]:
        """Resample OHLCV data to desired bar width.
        Specified width must be greater than original bar width.
        :param width: string timedelta ('15M', '1D', etc.) or pandas Timedelta object
        :param return_df: specify whether to return raw DataFrame, or another OHLCV object
        :return: DataFrame containing resampled data
        """

        # Verify input width
        width = pd.to_timedelta(width)
        if width < self.bar_width:
            raise ValueError("Resample width [{}] must be greater than original width [{}]".format(
                str(width), str(self.bar_width)))

        # Volume weighted column should be aggregated using bar volume for weight.
        # Calculate a dollar-volume column that can be aggregated directly, then re-compute vw.
        self.df['_dv'] = self.df['volume'] * self.df['vw']

        resampled_df = self.df.resample(width).agg(
            {'open': 'first',
             'high': 'max',
             'low': 'min',
             'close': 'last',
             'volume': 'sum',
             '_dv': 'sum'})

        # Compute aggregated vw
        resampled_df['vw'] = self.df['_dv'] / self.df['volume']

        # Drop helper variable used for vw computation from both DataFrames
        resampled_df.drop('_dv', axis=1, inplace=True)
        self.df.drop('_dv', axis=1, inplace=True)

        # Return desired object type
        if return_df:
            return resampled_df
        else:
            return OHLCV(self.symbol, resampled_df)

    @staticmethod
    def from_dataframe(symbol: str, ohlcv: pd.DataFrame) -> OHLCV:
        """Create OHLCV object from pandas DataFrame. Standardize column names and time data
        :param ohlcv: input pandas DataFrame
        :return: OHLCV object
        """

        # If index exist, assume it is time, assign to 'time' column
        if ohlcv.index[0] != 0:
            ohlcv['time'] = ohlcv.index
            ohlcv.reset_index(drop=True, inplace=True)

        # Standardize the OHLC column names
        # TODO Make this more robust
        ohlcv.columns = ohlcv.columns.str.lower().str.strip()
        ohlcv.rename(columns={
            'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 't': 'time',
            'vwap': 'vw'
        }, errors='ignore', inplace=True)

        # Standardize column names
        ohlcv.set_index('time', drop=True, inplace=True)
        ohlcv.index = pd.to_datetime(ohlcv.index)

        # Remove other columns
        _ = [ohlcv.drop(columns=col, inplace=True) for col in ohlcv.columns if col not in OHLCV.COLUMNS]

        # Create a volume weighted column if not present
        if 'vw' not in ohlcv.columns:
            ohlcv['vw'] = (ohlcv['open'] + ohlcv['high'] + ohlcv['low'] + 2 * ohlcv['close']) / 5

        # Create object
        return OHLCV(symbol, ohlcv)

    @staticmethod
    def from_list(symbol: str, ohlcv: List[Dict]) -> OHLCV:
        df = pd.DataFrame(ohlcv)
        return OHLCV.from_dataframe(symbol, df)

if __name__ == "__main__":
    import time
    api = AlpacaRestAPI()
    bars = api.get_bars(symbol='TSLA', timeframe=TimeFrame(5, TimeFrameUnit.Minute),
                        start='2021-12-17', adjustment='raw', limit=10000)

    list_d = [item._raw for item in bars]
    df = pd.DataFrame(list_d)
    df.set_index('t', drop=False, inplace=True)
    df.index = pd.to_datetime(df.index)
    df.columns = df.columns.str.lower()
    df2 = bars.df

    valid = isinstance(df2.index, pd.DatetimeIndex)
    valid2 = isinstance(df.index, str)

    ohlcv = OHLCV.from_dataframe('T', bars.df)
    rs_df = ohlcv.resample('1H')

    print(ohlcv.bar_duration)

    ohlcv.df.ta.log_return(cumulative=True, append=True)
    ohlcv.df.ta.sma(length=10, append=True)

    t0 = time.time()
    for i in range(4000):
        ohlcv.df.ta.alma(close='close', length=20, sigma=6, distribution_offset=0.85, append=True)
    t1 = time.time()
    print((t1-t0)/4000)
    ohlcv.df.to_csv("local.csv")


    print()
