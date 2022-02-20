#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The Alpaca module contains several utilities for interacting with the Alpaca trading platform.
"""
import asyncio
from enum import auto
import json
import logging
import threading
import time
import traceback
import alpaca_trade_api
import numpy as np
import uvloop

from typing import Dict, Callable

# Local imports
from neptune.config import NeptuneConfiguration
from neptune.common.universe import SymbolUniverse

from neptune.alpaca.utilities import ORDER_STATUS_TRANSFORM
from neptune.alpaca.api_types import Order
from neptune.alpaca.marketstore_api import MarketStoreApi
from neptune.alpaca.rest_api import AlpacaRestAPI

async def test_print():
    while True:
        logging.error("Message from test_print") 
        await asyncio.sleep(5)

class TradeUpdateStream:
    def __init__(self, autostart=False, query_period=20):
        self.positions = {}
        self.logger = logging.getLogger('TradeUpdateStream')
        
        self._callbacks = []
        self._stream = None
        self._event_loop = None
        self._alpaca_rest = AlpacaRestAPI()
        self._query_period = query_period
        self._last_query_time = 0.0
        
        if autostart:
            self.start()

    def start(self):
        """Start PositionManager processing thread."""
        #threading.Thread(target=self._run, daemon=True).start()
        self._run()
    
    def stop(self):
        pass
    
    def register_callback(self, fcn: Callable):
        """Add custom callback

        Args:
            fcn (Callable): _description_
        """
        self._callbacks.append(fcn)

    def get_positions(self) -> Dict[str, str]:

        async def _get_positions() -> dict:
            return self.positions.copy()

        result = {}
        try:
            fut = asyncio.run_coroutine_threadsafe(
                _get_positions(), loop=self.event_loop
            )
            result = fut.result()
        except Exception as e:
            self.logger.error(f"Error getting current positions: {e}")
        return result

    def _run(self):
        """Thread containing Alpaca websocket client event loop and some re-connection logic."""
        # Internal function with recursive call to handle re-connection.
        def run_connection():
            try:
                self.logger.info("Connecting to Alpaca websocket")
                self._stream.run()
            except Exception as e:
                self.logger.error(f"Exception from websocket connection: {e}")
            finally:
                self.logger.info("Trying to re-establish connection")
                time.sleep(3)
                run_connection()

        # Create event loop
        try:
            # make sure we have an event loop, if not create a new one
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        finally:
            self._event_loop = asyncio.get_event_loop()
            self._event_loop.set_debug(True)

        # Create a websocket connection
        # This is used to get asynchronous trade updates from Alpaca
        config = NeptuneConfiguration()
        self._stream = alpaca_trade_api.Stream(
            key_id=config.api_key, secret_key=config.secret_key, base_url=config.base_url 
        )

        # Subscribe to trade updates and add synchrounous position updating task to event loop
        self._stream.subscribe_trade_updates(self.handle_trade_update)
        asyncio.ensure_future(self.update_positions())

        # Call reconnection handler
        run_connection()

    async def handle_trade_update(self, msg: dict):
        """Asynchronous couroutine to handle trade updates received from Alpaca websocket.

        Args:
            msg (dict): trade update message
        """

        # Update the positions dict
        self._update_positions() 

    async def update_positions(self):
        """Synchronous coroutine to update position dictionary."""
        while True:
            last_query_dtime = time.time() - self._last_query_time
            if last_query_dtime >= self._query_period:
                print("Updating positions")
                self._update_positions()
            await asyncio.sleep(self._query_period)

    def _update_positions(self):
        """Internal class helper function to perform position updates."""

        # Store time we are querying the API
        self._last_query_time = time.time()

        # Get current positions using REST API client
        positions = self._alpaca_rest.list_positions()
        updated_symbols = list(positions.keys())

        # Get new symbols
        current_symbols = list(self.positions.keys())
        new_symbols = np.setdiff1d(updated_symbols, current_symbols)
        for sym in new_symbols:
            self.logger.info("New position {}".format(sym))

        # Get removed symbols
        removed_symbols = np.setdiff1d(current_symbols, updated_symbols)
        for sym in removed_symbols:
            self.positions.pop(sym)
            self.logger.info("Closed position {}".format(sym))

        # Overwrite dict with new positions
        self.positions = positions
    

class AlpacaWebSocket(alpaca_trade_api.Stream):
    """
    Singleton instance of Alpaca websocket
    """

    def __init__(self):
        config = NeptuneConfiguration()
        super().__init__(key_id=config.api_key,
                         secret_key=config.secret_key,
                         base_url=config.base_url,
                         data_feed='sip',
                         raw_data=True)

        # Set up logging
        self.logger = logging.getLogger('alpaca')

        # Create Marketstore API
        self.marketstore = MarketStoreApi()

        self.thr = threading.Thread(target=self._run, daemon=True)
        self.thr.start()

    def _run(self):
        """Thread containing Alpaca websocket client event loop and some re-connection logic."""
        # Internal function with recursive call to handle re-connection.
        def run_connection():
            try:
                self.logger.info("Connecting to Alpaca websocket")
                self.run()
            except Exception as e:
                self.logger.error(f"Exception from websocket connection: {e}")
            finally:
                self.logger.info("Trying to re-establish connection")
                time.sleep(3)
                run_connection()

        # Create event loop
        try:
            # make sure we have an event loop, if not create a new one
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        finally:
            self._event_loop = asyncio.get_event_loop()
            self._event_loop.set_debug(True)

        # Subscribe to trade updates and add synchrounous position updating task to event loop
        self.subscribe_trade_updates(self.process_order)
        asyncio.ensure_future(self.update_positions())

        # Call reconnection handler
        run_connection()
    

    def stop(self):
        """Stop all threads and close sockets.
        """
        self.logger.info("Stopping AlpacaWebSocket client.")
        self.event.set()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stop_ws())

    def _initialize(self):
        """Initializes object. Subscribes to data. Start heartbeat thread.
        """
        # Subscribe to Order updates

    async def update_positions(self):
        """Synchronous coroutine to update position dictionary."""
        while True:
            print("Updating positions")
            await asyncio.sleep(self._query_period)

    async def process_order(self, order_data: dict):
        """Process Order data
        :param order_data: https://alpaca.markets/docs/api-documentation/api-v2/orders/#order-entity
        """

        # Convert python dict to Order object, pack status in order dict for publishing
        order = order_data._raw['order']
        order['status'] = order_data._raw['event']
        self.logger.error("\n{}".format(json.dumps(order, indent=3)))

    @staticmethod
    def Run(kill_event=None):
        """Create and start AlpacaWebSocket. Monitor status of websockets and exit with code 1 if any
        websocket disconnects. A bash script should be used around this call to handle re-connection.
        @param kill_event:
        """

        def run_connection():
            try:
                aws.run()
            except KeyboardInterrupt:
                logging.info("Interrupted execution by user")
                asyncio.get_event_loop().run_until_complete(aws.stop_ws())
                exit(0)
            except Exception as e:
                traceback.print_exc()
                logging.error(f'Exception from websocket connection: {e}')
            finally:
                logging.error("Trying to re-establish connection")
                time.sleep(3)
                run_connection()

        # Create Alpaca Websocket client
        aws = AlpacaWebSocket()
        run_connection()


if __name__ == '__main__':

    #api = AlpacaWebSocket()

    #uvloop.install()

    
    ts = TradeUpdateStream(autostart=True)
    while True:
        time.sleep(10)
