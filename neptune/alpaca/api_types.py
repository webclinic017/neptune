
from __future__ import annotations
from enum import Enum
from typing import List
import pandas as pd
import logging
import json

logger = logging.getLogger()

class PositionSide(Enum):
    LONG = "long"
    SHORT = "short"

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"
    
class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    
class OrderStatus(Enum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    DONE_FOR_DAY = "done_for_day"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REPLACED = "replaced"
    PENDING_CANCEL = "pending_cancel"
    PENDING_RELEASE = "pending_release"
    ACCEPTED = "accepted"
    HELD = "held"
    PENDING_NEW = "pending_new"
    ACCEPTED_FOR_BIDDING = "accepted_for_bidding"
    STOPPED = "stopped"
    REJECTED = "rejected"
    SUSPENDED = "suspended"
    CALCULATED = "calculated"

class OrderTimeInForce(Enum):
    DAY = "day"
    GTC = "gtc"
    OPG = "opg"
    CLS = "cls"
    IOC = "ioc"
    FOK = "fok"

class OrderClass(Enum):
    SIMPLE = "simple"
    BRACKET = "bracket"
    OCO = "oco"
    OTO = "oto"
    default = SIMPLE

    @classmethod
    def _missing_(self, value):
        return OrderClass.SIMPLE

        
class ApiMessage:
    def __init__(self, msg: dict):
        self._raw = msg
        self._timestamp = pd.Timestamp.utcnow()
        
    def __getattr__(self, name: str):
        value = self._raw[name]
        if value:
            if name[-3:] == "_at":
                value = pd.to_datetime(value)
            elif not isinstance(value, bool):
                try:
                    value = float(value)
                except ValueError:
                    pass
        return value

    def __str__(self) -> str:
        """Return payload of Alpaca API Message"""
        return json.dumps(self._raw, indent=3)

    def __bool__(self) -> bool:
        """Return validity of message"""
        return self._raw is not None


class AccountMessage(ApiMessage):
    def __init__(self, msg: dict):
        """Create wrapper for Account message response from Alpaca API.

        Args:
            msg (dict): raw payload from Alpaca API
        """
        super().__init__(msg)


class PositionMessage(ApiMessage):
    def __init__(self, msg: dict):
        """Create wrapper for Position message response from Alpaca API.

        Args:
            msg (dict): raw payload from Alpaca API
        """
        super().__init__(msg)

    @property
    def side(self) -> PositionSide:
        return PositionSide(self._raw['side'])
        

class OrderMessage(ApiMessage):
    def __init__(self, msg: dict):
        """Create wrapper for Order message response from Alpaca API.

        Args:
            msg (dict): raw payload from Alpaca API
        """
        # Extract order dict from websocket message
        payload = None
        if 'stream' in msg.keys():
            payload = msg.get('data', {}).get('order', None)
            if payload is None:
                logger.error("Invalid Order message structure: {}".format(json.dumps(msg, indent=3)))
        elif 'id' in msg.keys():
            payload = msg

        # Store raw message
        super().__init__(payload)

    @property
    def side(self) -> OrderSide:
        return OrderSide(self._raw['side'])

    @property 
    def type(self) -> OrderType:
        return OrderType(self._raw['type'])

    @property
    def status(self) -> OrderStatus:
        return OrderStatus(self._raw['status'])

    @property
    def time_in_force(self) -> OrderTimeInForce:
        return OrderTimeInForce(self._raw['time_in_force'])
    
    @property
    def order_class(self) -> OrderClass:
        return OrderClass(self._raw['order_class'])

    @property
    def legs(self) -> List[OrderMessage]:
        if self._raw['legs'] is None:
            return []
        else:
            return [OrderMessage(leg) for leg in self._raw['legs']]

    def get_response_latency(self) -> float:
        """Calculates latency of Order message from Alpaca API.

        Returns:
            float: message latency [seconds]
        """
        return (self._timestamp - self.updated_at).microseconds / 1e6

if __name__ == '__main__':
    order = OrderMessage({"stream": "dog", "a": 1, "b": "4"})
    
    legs = order.legs
    
    print()