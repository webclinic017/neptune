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

# Logger
_logger = logging.getLogger(__name__)

# Alpaca key conversions =========================================================================
# Used for converting received keys to protobuf message definition keys so MessageToDict can be used
BAR_TRANSFORM = {'S': 'sym'}
TRADE_TRANSFORM = {'S': 'sym', 'x': 'exchange', 'p': 'price', 's': 'size', 'z': 'tape'}
ORDER_STATUS_TRANSFORM = {
    'fill': 'filled',
    'partial_fill': 'partially_filled',
    'order_replace_rejected': 'rejected',
    'order_cancel_rejected': 'canceled'}

# Alpaca type JSON conversions ====================================================================
account_conversion = {'buying_power': float,
                      'regt_buying_power': float,
                      'daytrading_buying_power': float,
                      'non_marginable_buying_power': float,
                      'cash': float,
                      'accrued_fees': float,
                      'pending_transfer_in': float,
                      'portfolio_value': float,
                      'multiplier': int,
                      'equity': float,
                      'last_equity': float,
                      'long_market_value': float,
                      'short_market_value': float,
                      'initial_margin': float,
                      'maintenance_margin': float,
                      'last_maintenance_margin': float,
                      'sma': float,
                      'daytrade_count': int}

order_conversion = {'qty': int,
                    'filled_qty': int,
                    'filled_avg_price': float,
                    'limit_price': float,
                    'stop_price': float,
                    'trail_percent': float,
                    'trail_price': float,
                    'hwm': float}

position_conversion = {'avg_entry_price': float,
                       'qty': float,
                       'market_value': float,
                       'cost_basis': float,
                       'unrealized_pl': float,
                       'unrealized_plpc': float,
                       'unrealized_intraday_pl': float,
                       'unrealized_intraday_plpc': float,
                       'current_price': float,
                       'lastday_price': float,
                       'change_today': float}


# Conversion functions ============================================================================
def format_account(account: dict) -> dict:
    """Converts string parameter of JSON account response to numeric
    @param account: dict containing raw account REST response
    @returns: account dict with numeric converted fields
    """
    try:
        # Use the account_conversion dict to perform conversions
        for field, dtype in account_conversion.items():
            account[field] = dtype(account[field]) if account[field] else None
    except Exception as e:
        _logger.error("Exception caught in formatting Alpaca order data: {}".format(e))
    return account


def format_order(order: dict) -> dict:
    """Convert string parameters of JSON response to numeric.
    @param order: dict containing order parameters
    @return: order dict with numeric converted fields
    """
    try:
        # Use the order_conversion dict to perform conversions
        for field, dtype in order_conversion.items():
            order[field] = dtype(order[field]) if order[field] else None
        # If Order has legs, call recursively
        if order['legs']:
            for i, leg in enumerate(order['legs']):
                order['legs'][i] = format_order(order=leg)
    except Exception as e:
        _logger.error("Exception caught in formatting Alpaca order data: {}".format(e))
    return order


def format_position(position: dict) -> dict:
    """Convert string parameters of JSON response to numeric.
    @param position: dict containing position dicts
    @return: position dict with numeric converted fields
    """
    try:
        # Use the order_conversion dict to perform conversions
        for field, dtype in position_conversion.items():
            position[field] = dtype(position[field]) if position[field] else None
        # Convert percentages
        position['unrealized_plpc'] *= 100
        position['unrealized_intraday_plpc'] *= 100
        position['change_today'] *= 100
    except Exception as e:
        _logger.error("Exception caught in formatting Alpaca position data: {}".format(e))
    return position


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
