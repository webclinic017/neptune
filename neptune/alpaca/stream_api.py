#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The Alpaca module contains several utilities for interacting with the Alpaca trading platform.
"""
import asyncio
import json
import logging
import threading
import time
import traceback
import alpaca_trade_api
from google.protobuf.json_format import ParseDict

# Local imports
from alpaca.interface.generated.interface_pb2 import Order, Aggregate, Trade
from alpaca.interface.utilities import proto_utilities
from alpaca.api.utilities import ORDER_STATUS_TRANSFORM
from alpaca.conf.config import AlpacaMsConfiguration, configure_logging
from alpaca.api.marketstore_api import MarketStoreApi
from alpaca.common.universe import SymbolUniverse


class AlpacaWebSocket(alpaca_trade_api.Stream):
    """
    Singleton instance of Alpaca websocket
    """

    def __init__(self):
        config = AlpacaMsConfiguration()
        super().__init__(key_id=config.api_key,
                         secret_key=config.secret_key,
                         base_url=config.base_url,
                         data_feed='sip',
                         raw_data=True)

        # Set up logging
        self.logger = logging.getLogger('alpaca')

        # Create Marketstore API
        self.marketstore = MarketStoreApi()

        # Configure socket for publishing
        self.logger.info("Creating Order socket")
        proto_parser = proto_utilities.ProtoParser()
        # self.data_sockets = {'order': proto_parser.get_socket('Order', sub=False, bind=True),
        #                     'bar': proto_parser.get_socket('Aggregate', sub=False, bind=True),
        #                     'trade': proto_parser.get_socket('Trade', sub=False, bind=True),
        #                     'crypto': None}
        self.aggregate = Aggregate()
        self.trade = Trade()
        self.event = threading.Event()
        self._initialize()

    def stop(self):
        """Stop all threads and close sockets.
        """
        self.logger.info("Stopping AlpacaWebSocket client.")
        self.event.set()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stop_ws())
        # _ = [socket.close() for socket in self.data_sockets.values()]
        self.heartbeat.join()

    def _initialize(self):
        """Initializes object. Subscribes to data. Start heartbeat thread.
        """
        # Subscribe to Order updates
        self.subscribe_trade_updates(self.process_order)

        # Subscribe to all minute aggregate data
        symbols = list(SymbolUniverse().data.index)
        for sym in symbols:
            self.subscribe_bars(self.process_bar_data, sym)

        # Subscribe to crypto pairs
        self.subscribe_crypto_bars(self.process_crypto, '*')
        self.subscribe_crypto_bars(self.process_crypto2, '*')

        self.bar_time = dict()
        self.msg_counts = {'order': 0, 'bar': 0, 'trade': 0, 'crypto': 0}
        self.heartbeat = threading.Thread(target=self.log_status, daemon=False)

        # Start threads
        self.heartbeat.start()

    async def process_crypto(self, crypto_data: dict):
        self.msg_counts['crypto'] += 1
        print("Function1")
        
    async def process_crypto2(self, crypto_data: dict):
        self.msg_counts['crypto'] += 1
        print("Function2")

    async def process_order(self, order_data: dict):
        """Process Order data
        :param order_data: https://alpaca.markets/docs/api-documentation/api-v2/orders/#order-entity
        """

        # Increment counter
        self.msg_counts['order'] += 1

        # Convert python dict to Order object, pack status in order dict for publishing
        order = order_data._raw['order']
        order['status'] = order_data._raw['event']
        self.logger.debug("\n{}".format(json.dumps(order, indent=3)))

        # Translate stream return values to fit proto definition
        if order['status'] in ORDER_STATUS_TRANSFORM.keys():
            order['status'] = ORDER_STATUS_TRANSFORM[order['status']]
        msg = ParseDict(order, Order(), ignore_unknown_fields=True)

        # Publish on socket
        topic = msg.symbol
        # self.data_sockets['order'].send_multipart([topic.encode(), msg.SerializeToString()])

    async def process_bar_data(self, bar_data: dict):
        """Process Aggregate (bar) data
        :param bar_data: https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/historical/#bars
        """

        # Increment counter
        self.msg_counts['bar'] += 1
        symbol = bar_data['S']
        time_ms = int(time.time_ns() / 1e6)

        # Write to Marketstore
        self.marketstore.insert(bar_data)

        # Pack protobuf message
        self.aggregate.Clear()
        self.aggregate.hdr.utc_time = int((bar_data['t'].seconds + bar_data['t'].nanoseconds / 1e9) * 1e3)
        self.aggregate.ev = 'AM'
        self.aggregate.sym = symbol
        self.aggregate.v = bar_data['v']
        self.aggregate.o = bar_data['o']
        self.aggregate.h = bar_data['h']
        self.aggregate.l = bar_data['l']
        self.aggregate.c = bar_data['c']
        self.aggregate.vw = bar_data['vw']
        self.aggregate.s = int(self.bar_time.get(self.aggregate.sym, time_ms - 6e4))
        self.aggregate.e = time_ms

        # Store end time for next bar
        self.bar_time[symbol] = time_ms

        # Publish on socket, use symbol as topic name
        # self.data_sockets['bar'].send_multipart(
        #    [symbol.encode(), self.aggregate.SerializeToString()])

    async def process_trade_data(self, trade_data: dict):
        """Process Trade data
        :param trade_data: https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/historical/#trades
        """

        # Increment counter
        self.msg_counts['trade'] += 1
        symbol = trade_data['S']

        # Pack protobuf message
        self.trade.Clear()
        self.trade.hdr.utc_time = int(time.time_ns() / 1e6)
        self.trade.sym = symbol
        self.trade.exchange = trade_data['x']
        self.trade.price = trade_data['p']
        self.trade.size = trade_data['s']
        self.trade.timestamp = int(trade_data['t'].seconds * 1e9 + trade_data['t'].nanoseconds)
        self.trade.tape = trade_data['z']
        _ = [self.trade.conditions.append(c.encode('utf-8')) for c in trade_data['c']]

        # Publish on socket, use symbol as topic name
        # self.data_sockets['trade'].send_multipart(
        #    [symbol.encode(), self.trade.SerializeToString()])

    def log_status(self):
        """Logs websocket status and message count every 30 seconds.
        """
        streams = {'order': self._trading_ws,
                   'bar': self._data_ws,
                   'crypto': self._crypto_ws}

        # Display initial status message
        for name, stream in streams.items():
            while True:
                try:
                    self.logger.info("[{}] Host: {} | Client: {} | Port: {} | Status: {}".format(
                        name, stream._endpoint, stream._ws.local_address[0], stream._ws.port, stream._running))
                    break
                except Exception as e:
                    self.logger.info("Waiting for {} connection...".format(name))
                    time.sleep(3)

        # Start loop at 10 or 40 second mark for heartbeat message
        sec = time.time() % 60
        time.sleep(min([abs(10 - sec), abs(40 - sec)]))
        while not self.event.is_set():
            logstr = []
            for name, stream in streams.items():
                state = "Running" if stream._running else "**Disconnected**"
                logstr.append("[{}] {} | Count: {}".format(name, state, self.msg_counts[name]))
                self.msg_counts[name] = 0
            self.logger.info(" || ".join(logstr))
            time.sleep(30)

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
    configure_logging()
    AlpacaWebSocket.Run()

    print()
