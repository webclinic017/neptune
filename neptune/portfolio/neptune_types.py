
from __future__ import annotations
from typing import Dict, List

from neptune.alpaca import api_types

class Order:
    def __init__(self, msg: api_types.OrderMessage):
        self.parent = None
        self._raw = {}
    
    @staticmethod
    def from_alpaca_order(msg: api_types.OrderMessage) -> List[str, Order]:
        orders = [msg]
        if msg.legs is not None and len(msg.legs) > 0:
            orders += list(msg.legs.values())
        
        print()

class OrderManager:
    def __init__(self):
        self.orders = {}
        
    def update(self, order: Order) -> bool:
         





                 