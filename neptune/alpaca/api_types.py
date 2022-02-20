
from __future__ import annotations
from enum import Enum
from typing import List
import pandas as pd

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

class Order:
    def __init__(self, msg: dict):
        
        self.id: str = None
        self.client_id: str = None
        self.symbol: str = None

        self.update(msg)

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
    def legs(self) -> List[Order]:
        if self._raw['legs'] is None:
            return []
        else:
            return [Order(leg) for leg in self._raw['legs']]
    
    def __getattr__(self, name: str):
        value = self._raw.get[name]
        if value:
            if name[-3:] == "_at":
                value = pd.to_datetime(value)
        return value
        
    def update(self, msg: dict) -> bool:
        # Extract order dict from websocket message
        if 'stream' in msg.keys():
            msg = msg.get('data', {}).get('order', None)
            if msg is None:
                return False

        # Store raw message
        self._raw = msg
        return True

