#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The Alpaca module contains several utilities for interacting with the Alpaca trading platform.
"""

import datetime as dt
import logging
from typing import Optional

import alpaca_trade_api
import numpy as np
from pytz import timezone

# Local packages
from neptune.config import NeptuneConfiguration
from neptune.alpaca.api_types import OrderMessage, PositionMessage, AccountMessage


class AlpacaRestAPI(alpaca_trade_api.REST):
    """
    Derived class of official Alpaca REST API with some enhanced functionality
    https://github.com/alpacahq/alpaca-trade-api-python/blob/03bb3e9153/alpaca_trade_api/rest.py
    """

    def __init__(self):
        config = NeptuneConfiguration()
        super().__init__(key_id=config.api_key,
                         secret_key=config.secret_key,
                         base_url=config.base_url,
                         api_version='v2',
                         raw_data=True)
        self.logger = logging.getLogger(__class__.__name__)

    def get_seconds_until_market_open(self):
        """Return the time in seconds until the market opens next.
        https://alpaca.markets/docs/api-documentation/api-v2/clock/#get-the-clock
        """
        time_until_open = 0
        clock = self.get_clock()
        if not clock['is_open']:
            utc_tz = timezone('UTC')
            next_open = dt.datetime.fromisoformat(clock['next_open']).astimezone(utc_tz)
            now = dt.datetime.now().astimezone(utc_tz)
            time_until_open = (next_open - now).seconds
        return time_until_open

    def get_limit_price(self, symbol: str, seconds=30) -> float:
        """Get limit price for symbol using recent trades, or latest trade
        @param symbol: symbol to get limit price for
        @param seconds: number of seconds to average
        @return: float of limit price
        """
        try:
            self.logger.debug("[get_limit_price] API: get_trades | Symbol: {} | Seconds: {}".format(symbol, seconds))
            end = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            start = end - dt.timedelta(seconds=seconds)
            trades = self.get_trades(symbol=symbol, start=start.isoformat(), end=end.isoformat()).df
            price = np.average(trades['price'], weights=trades['size'])
        except Exception as e:
            self.logger.debug("[get_limit_price] API: get_latest_trade | Symbol: {}".format(symbol, seconds))
            trade = self.get_latest_trade(symbol=symbol)
            price = trade['p']
        return price

    def get_account(self) -> dict:
        """Get account info
        https://alpaca.markets/docs/api-documentation/api-v2/account/#account-entity
        """
        self.logger.debug("[get_account]")
        account = super().get_account()
        return AccountMessage(account)

    def list_orders(self, **kwargs) -> dict:
        """Wrapper around list_orders to convert Order objects to list
        https://alpaca.markets/docs/api-documentation/api-v2/orders/#get-a-list-of-orders
        @param kwargs: same arguments as superclass list_orders
        @return: list of order dicts
        """
        self.logger.debug("[list_orders] {}".format(kwargs))
        order_l = super().list_orders(**kwargs)
        return {order['id']: OrderMessage(order) for order in order_l}

    def submit_order(self, **kwargs) -> dict:
        """Wrapper around submit_order to convert Order object to dict
        https://alpaca.markets/docs/api-documentation/api-v2/orders/#request-a-new-order
        @param kwargs: same arguments as superclass list_orders
        @return: order dict
        """
        self.logger.debug("[submit_order] {}".format(kwargs))
        order = super().submit_order(**kwargs)
        return OrderMessage(order)

    def cancel_order_by_symbol(self, symbol: str):
        """Cancel all open orders for a symbol.
        @param symbol: selected symbol to cancel orders for
        """
        orders = self.list_orders(status='open')
        for k, v in orders.items():
            if v['symbol'] == symbol:
                self.cancel_order(order_id=k)

    def close_position(self, symbol: str) -> Optional[OrderMessage]:
        """Wrapper around list_orders to convert Order objects to list
        https://alpaca.markets/docs/api-documentation/api-v2/positions/#close-a-position
        @return: list of order dicts
        :param **kwargs:
        """
        try:
            self.logger.debug("[close_position] Symbol: {}".format(symbol))
            return OrderMessage(super().close_position(symbol=symbol))
        except alpaca_trade_api.rest.APIError as e:
            self.logger.error('[close_position] Symbol: {} | Exception: {}'.format(symbol, e))
            return None

    def list_positions(self) -> dict:
        """ Return dict format of positions
        https://alpaca.markets/docs/api-documentation/api-v2/positions/#get-open-positions
        @return: positions dict
        """
        self.logger.debug("[list_positions]")
        position_l = super().list_positions()
        return {pos['symbol']: PositionMessage(pos) for pos in position_l}

    def get_position(self, symbol: str) -> Optional[dict]:
        """Get current position for given symbol.
        https://alpaca.markets/docs/api-documentation/api-v2/positions/#get-an-open-position
        @param symbol: symbol to retrieve position for
        @return: Position object if valid, else None
        """
        try:
            self.logger.debug("[get_position] Symbol: {}".format(symbol))
            position = super().get_position(symbol)
            return PositionMessage(position)
        except alpaca_trade_api.rest.APIError as e:
            self.logger.error("[get_position] Symbol: {} | Exception: {}".format(symbol, e))
            return None

    def list_tradable_assets(self, require_shortable=False) -> set:
        """Return list of tradable assets
        https://alpaca.markets/docs/api-documentation/api-v2/assets/#get-assets
        @param require_shortable: Only return tickers that are shortable with Alpaca
        @return: dict of tickers
        """
        assets = self.list_assets(status='active')

        asset_d = {
            a['symbol']: a for a in assets if (
                    a['tradable'] and a['marginable'] and (not require_shortable or a['shortable']))
        }
        return {a.pop('symbol') for a in asset_d.values()}

if __name__ == '__main__':
    api = AlpacaRestAPI()
    
    order = api.submit_order(
        symbol='SPY', type='limit', limit_price=434, side='buy', qty=10, time_in_force='gtc',
        order_class='bracket',
        take_profit={
            'limit_price': 440
        },
        stop_loss={
            'stop_price': 432,
            'limit_price': 430
        } )

    positions = api.list_positions()
    orders = api.list_orders()
    account = api.get_account()
    print()
