#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import asyncio
import uvloop
import logging
import threading
import numpy as np
import pandas as pd
import pandas_ta as ta
from typing import Dict

import alpaca_trade_api as alpaca

# API Keys ====================================================================
API_KEY = "PKWMCUZB4QIK4NNOLR37"
API_SECRET = "3t27JWged7fcjLVAdaGBpdSumXXmZ7mecu2741GJ"
APCA_API_BASE_URL = "https://paper-api.alpaca.markets"


# Configure logging ===========================================================
# https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
LOG_LEVEL = logging.INFO
logging.basicConfig(filename="alpaca-demo.log", filemode="w", level=LOG_LEVEL)
logging.getLogger().addHandler(logging.StreamHandler())

# Alpaca REST API Client ======================================================
# https://github.com/jaggas/alpaca-trade-api-python
REST_API = alpaca.REST(
    key_id=API_KEY, secret_key=API_SECRET, base_url=APCA_API_BASE_URL, api_version="v2"
)

# Symbol list =================================================================
SYMBOL_LIST = ["BAC", "S", "NVDA", "CPX", "VZ", "DAL", "TSLA"]


class Position:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.active = False
        self.data = None

    def update(self, msg: alpaca.rest.Position):
        self.active = True
        self.data = msg

    def close(self):
        self.active = False
        self.data = None

    def process(self):
        bars_1D = REST_API.get_bars(
            symbol=self.symbol,
            timeframe=alpaca.TimeFrame(1, alpaca.TimeFrameUnit.Day),
            start=(pd.Timestamp.now() - pd.Timedelta(days=800)).strftime("%Y-%m-%d"),
            limit=1000,
            adjustment='all'
        ).df

        bars_1h = REST_API.get_bars(
            symbol=self.symbol,
            timeframe=alpaca.TimeFrame(1, alpaca.TimeFrameUnit.Hour),
            start=(pd.Timestamp.now() - pd.Timedelta(days=25)).strftime("%Y-%m-%d"),
            limit=1000,
            adjustment='split'
        ).df

        bars_15min = REST_API.get_bars(
            symbol=self.symbol,
            timeframe=alpaca.TimeFrame(15, alpaca.TimeFrameUnit.Minute),
            start=(pd.Timestamp.now() - pd.Timedelta(days=10)).strftime("%Y-%m-%d"),
            limit=1000,
            adjustment='split'
        ).df

        # Process 1H bars
        bars_1h.ta.bbands(length=20, std=1, append=True)
        bars_1h.ta.bbands(length=20, std=2, append=True)
        bars_1h.ta.bbands(length=20, std=3, append=True)
        bars_1h.ta.bbands(length=20, std=4, append=True)
        compute_alma_trend(bars_1h)

        # Process 1D bars
        bars_1D.ta.bbands(length=20, std=1, append=True)
        bars_1D.ta.bbands(length=20, std=2, append=True)
        bars_1D.ta.bbands(length=20, std=3, append=True)
        bars_1D.ta.bbands(length=20, std=4, append=True)
        compute_alma_trend(bars_1D, length=40)

        print() # PUT BREAKPOINT HERE to see dataframes
        # TODO Put logic for trading here


class PositionManager:
    def __init__(self, autostart=False):
        """Class to track and update current positions

        Args:
            autostart (bool, optional): Start position manager thread. Defaults to False.
        """

        # Initialize positions using a dict comprehension
        # https://www.programiz.com/python-programming/dictionary-comprehension
        self.positions = {pos.symbol: pos for pos in REST_API.list_positions()}

        if autostart:
            self.start()

    def start(self):
        """Start PositionManager processing thread."""
        threading.Thread(target=self._run).start()

    def get_positions(self) -> Dict[str, alpaca.rest.Position]:
        """Get current position dictionary.

        Returns:
            Dict[str, alpaca.rest.Position]: current positions
        """

        async def _get_positions() -> dict:
            return self.positions.copy()

        result = {}
        try:
            fut = asyncio.run_coroutine_threadsafe(
                _get_positions(), loop=self.event_loop
            )
            result = fut.result()
        except Exception as e:
            logging.error(f"Error getting current positions: {e}")
        return result

    def _run(self):
        """Thread containing Alpaca websocket client event loop and some re-connection logic.py
        Don't worry too much about this function. It is somewhat complicated and handles all of
        the event loop maintainence and websocket re-connection in event of a disconnect.
        Basically think of it as a thread that is doing two things:
            1) Receive and handle trade updates sent from Alpaca server via websocket (handle_trade_update)
            2) Every 20 seconds, call the REST API to update the state of the current positions.
        """
        # Internal function with recursive call to handle re-connection.
        def run_connection():
            try:
                logging.info("Connecting to Alpaca websocket")
                self.stream.run()
            except Exception as e:
                logging.error(f"Exception from websocket connection: {e}")
            finally:
                logging.info("Trying to re-establish connection")
                time.sleep(3)
                run_connection()

        # Create event loop
        try:
            # make sure we have an event loop, if not create a new one
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        finally:
            self.event_loop = asyncio.get_event_loop()
            self.event_loop.set_debug(True)

        # Create a websocket connection
        # This is used to get asynchronous trade updates from Alpaca
        self.stream = alpaca.Stream(
            key_id=API_KEY, secret_key=API_SECRET, base_url=APCA_API_BASE_URL
        )

        # Subscribe to trade updates and add synchrounous position updating task to event loop
        self.stream.subscribe_trade_updates(self.handle_trade_update)
        asyncio.ensure_future(self.update_positions())

        # Call reconnection handler
        run_connection()

    async def handle_trade_update(self, msg: dict):
        """Asynchronous couroutine to handle trade updates received from Alpaca websocket.

        Args:
            msg (dict): trade update message
        """
        logging.info("Received trade update")
        logging.info(str(msg))
        self._update_positions()  
        print() # PUT BREAKPOINT HERE to catch message from Alpaca websocket

    async def update_positions(self):
        """Synchronous coroutine to update position dictionary."""
        POSITION_UPDATE_INTERVAL = (
            10  # How often to update positions in synchronous thread
        )
        while True:
            print("Updating positions")
            self._update_positions()
            await asyncio.sleep(POSITION_UPDATE_INTERVAL)

    def _update_positions(self):
        """Internal class helper function to perform position updates.
        Not meant to be called outside of the class.
        """

        # Get current positions using REST API client
        positions = REST_API.list_positions()
        updated_symbols = [pos.symbol for pos in positions]

        # Get new symbols
        current_symbols = list(self.positions.keys())
        new_symbols = np.setdiff1d(updated_symbols, current_symbols)
        for sym in new_symbols:
            logging.info("New position {}".format(sym))

        # Get removed symbols
        removed_symbols = np.setdiff1d(current_symbols, updated_symbols)
        for sym in removed_symbols:
            self.positions.pop(sym)
            logging.info("Closed position {}".format(sym))

        for pos in positions:
            self.positions[pos.symbol] = pos


# Helper functions ============================================================
def format_position_msg(msg: dict) -> dict:
    result = {}
    for k, v in msg.items():
        if isinstance(v, str):
            try:
                result[k] = float(v)
            except:
                result[k] = v
        else:
            result[k] = v
    return result


def compute_alma_trend(df: pd.DataFrame, length=50, sigma=6, offset=0.85):
    """Compute and add ALMA indicator and ALMA trend to dataframe.
    Two new columns added to input dataframe:
        1) ALMA_{length}_{offset}_{sigma}
        2) ALMA_{length}_{offset}_{sigma}_trend

    Args:
        df (pd.DataFrame): input OHLCV dataframe
        length (int, optional): ALMA length. Defaults to 50.
        sigma (int, optional): ALMA sigma. Defaults to 6.
        offset (float, optional): ALMA offset. Defaults to 0.85.
    """
    # Determine truncation
    price = df.iloc[-1]["close"]
    if price < 10:
        n_decimal = 4
    elif price < 50:
        n_decimal = 3
    elif price < 150:
        n_decimal = 2
    else:
        n_decimal = 1

    atr = df.ta.atr(30)
    alma = df.ta.alma(length=length, sigma=sigma, offset=offset)
    d_alma = (0.75 * alma.diff(1) + 0.25 * alma.diff(1).shift(1)).round(n_decimal)

    sma = df.ta.sma(200)
    atr_band = atr / sma
    d_alma_norm = (d_alma / sma * 100).round(n_decimal)

    alma_tag = "ALMA_{}_{}_{}".format(length, sigma, round(offset, 2))
    alma_trend_tag = alma_tag + "_trend"
    df[alma_tag] = alma
    df[alma_trend_tag] = 0

    idx_down = d_alma_norm < -atr_band
    idx_up = d_alma_norm > atr_band
    df.loc[idx_down, alma_trend_tag] = -1
    df.loc[idx_up, alma_trend_tag] = 1


if __name__ == "__main__":

    # Dont worry about this, it is just a library that does some optimization in
    # the background
    uvloop.install() 

    # Cancel all orders to start
    REST_API.cancel_all_orders()

    # Working on the Position class currently, you can experiment 
    # with it by putting a break point where noted above.
    pos = Position("T")
    pos.process()

    # This creates and starts a PositionManager object. It is non-blocking, meaning
    # that it will start a thread in the background and let the program continue.
    # Something else to try is seeing the program  
    pm = PositionManager(autostart=True)

    time.sleep(5)
    while True:
        time.sleep(15)


