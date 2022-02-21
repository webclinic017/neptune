#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" ***********************************************************************************************
File:     utilities.py
Summary:  Various utility functions used for Alpaca API integration

@todo 
***********************************************************************************************"""

import re

import alpaca_trade_api
import logging
import datetime as dt

from neptune.alpaca.api_types import PositionSide

# Logger
_logger = logging.getLogger(__name__)

# Alpaca key conversions =========================================================================
# Used for converting received keys to protobuf message definition keys so MessageToDict can be used
BAR_TRANSFORM = {'S': 'sym'}
TRADE_TRANSFORM = {'S': 'sym', 'x': 'exchange', 'p': 'price', 's': 'size', 'z': 'tape'}

def format_bar(bar: dict) -> dict:
    bar['sym'] = bar.pop('S')

# Time functions ==================================================================================
def is_normal_hours() -> bool:
    """Return if normal trading hours"""
    now = dt.datetime.now().time()
    open = dt.time(9, 30, 0)
    close = dt.time(16, 00, 0)
    return True if open < now < close else False


def is_extended_hours() -> bool:
    """Return if extended trading hours"""
    now = dt.datetime.now().time()
    return is_premarket_hours() or is_after_hours()


def is_premarket_hours() -> bool:
    """Return if premarket trading hours"""
    now = dt.datetime.now().time()
    start = dt.time(4, 0, 0)
    open = dt.time(9, 30, 0)
    return True if start < now < open else False


def is_after_hours() -> bool:
    """Return if after hours trading"""
    now = dt.datetime.now().time()
    close = dt.time(16, 0, 0)
    end = dt.time(20, 0, 0)
    return True if close < now < end else False


def time_until_close() -> float:
    """Get number of minutes until market close"""
    now = dt.datetime.now()
    close = dt.datetime.combine(now.date(), dt.time(16, 0, 0))
    return (close - now).total_seconds() / 60


def get_timeframe(timeframe: str) -> alpaca_trade_api.TimeFrame:
    match = re.compile("[^\W\d]").search(timeframe)
    number = int(s[:match.start()])
    unit = s[match.start():]

    if upper(unit[0]) == "D":
        return alpaca_trade_api.TimeFrame(number, alpaca_trade_api.TimeFrameUnit.Day)
    elif upper(unit[0]) == "H":
        return alpaca_trade_api.TimeFrame(number, alpaca_trade_api.TimeFrameUnit.Hour)
    elif upper(unit[0]) == "M":
        return alpaca_trade_api.TimeFrame(number, alpaca_trade_api.TimeFrameUnit.Minute)
    else:
        raise ValueError("Timeframe unit not valid: {}".format(unit))
