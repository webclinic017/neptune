#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Various utility functions and classes for working with the protobuf/ZMQ interface layer
"""

import json
import logging
import re
import os

test = os.environ

import psutil
import zmq
from google.protobuf.json_format import MessageToDict

# Local imports
from neptune.interface.generated import interface_pb2 as pb2
from neptune.config import NEPTUNE_ROOT

# Find entire message block within proto file
MESSAGE_BLOCK = re.compile(r"/*(.*?});?", re.MULTILINE | re.DOTALL)

# Find only message definition block within single message block
MESSAGE_DEF = re.compile(r"message\s+(\w+)\s+{(.*?)\}", re.MULTILINE | re.DOTALL)

# Match components of single field within a protobuf message definition
FIELD_DEF = re.compile(r"\s+(repeated)*\s+(\w+)\s*(\w+)\s*=\s*(\d+)\s*;\s*(//(.*))?")


class ProtoParser:
    """Utility class to parse interface proto definition and construct a JSON definition file"""

    def __init__(self):
        self.logger = logging.getLogger("ProtoParser")
        self.proto_file = NEPTUNE_ROOT / 'neptune/interface/interface.proto'
        self.message_dict = dict()
        self._parse_proto()

    def lookup(self, message: str) -> dict:
        """Lookup protobuf message by name and return parameters
        @param message: protobuf message name string
        @return: dict containing message info
        """
        return self.message_dict.get(message, None)

    def get_socket(self, message: str, sub=True, bind=False) -> zmq.sugar.socket.Socket:
        """Create a pub/sub socket for the given message.
        The port number is managed within the function using the .proto comment block
        @param message: protobuf message name
        @param sub: True if creating a Subscriber, False for Publisher
        @param bind: True if port should bind
        @return: ZMQ socket for specified message
        """
        # Lookup message
        msg_data = self.lookup(message)
        if msg_data:
            opt = zmq.SUB if sub else zmq.PUB
            sock = zmq.Context().socket(opt)
            if bind:
                sock.bind('tcp://127.0.0.1:{}'.format(msg_data['Port']))
            else:
                sock.connect('tcp://127.0.0.1:{}'.format(msg_data['Port']))

            if sub:
                sock.subscribe('')

            self.logger.debug("[get_socket] Message: {} | Port: {} | Subscriber: {} | Bind: {}".format(
                msg_data['Name'], msg_data['Port'], sub, bind))
        else:
            sock = None
            self.logger.error("[get_socket] Could not load message data for {}".format(message))

        return sock

    def _parse_proto(self):
        """Read and parse the proto file"""
        # Read raw proto file
        with open(self.proto_file, 'r') as fp:
            raw_txt = fp.read()

        # Get all messages in proto file. Parse each one and store in object dict
        messages = MESSAGE_BLOCK.findall(raw_txt)
        for m in messages:
            if "@message" in m:
                self._parse_message(m)

    def _parse_message(self, input: str):
        """
        Parses a raw string containing a single interface message
        @param input:  raw string containing comment markups and proto message definition
        """
        # Parse the comment markdown section
        message = ProtoParser._get_parameter(input, 'message')
        port = ProtoParser._get_parameter(input, 'port')
        channel = ProtoParser._get_parameter(input, 'channel')
        summary = ProtoParser._get_parameter(input, 'summary')

        # Parse the message definition section, store in object dict
        structure = MESSAGE_DEF.findall(input)[0]
        type_name = structure[0]
        self.message_dict[type_name] = \
            {
                'Name': message,
                'Port': port,
                'Channel': channel,
                'Summary': summary,
            }

    @staticmethod
    def _parse_fields(input: str) -> dict:
        """
        Parses message fields from raw string
        :param input: raw string containing only message fields
        :return dict: containing message field data
        """
        field_dict = dict()
        # Split fields by newlines
        fields = filter(None, input.split(sep='\n'))

        # For each field, parse various components and store in dict
        for f in fields:
            d = FIELD_DEF.findall(f)[0]
            repeated = False if not d[0] else True
            vname = d[2]  # 4th element in regex group

            # Store field data
            field_dict[vname] = \
                {'Type': d[1], 'Enum': d[3], 'Repeated': repeated, 'Summary': d[4].strip()}
        return field_dict

    @staticmethod
    def _get_parameter(input: str, token: str) -> str:
        """
        Returns a parameter from a raw string that is denoted with a "@" in the proto comment markdown
        :param input: raw string containing parameter definition somewhere in it
        :param token: the parameter to return
        :return value of parameter
        """
        data = (re.findall(rf'@{token}\s+([^@*]*)', input, re.MULTILINE | re.DOTALL)[0]).strip().replace('\n', '')
        return re.sub(r'\s+', ' ', data)  # Replace multiple spaces with 1


def get_order_string(order: pb2.Order) -> dict:
    """Convert numeric message values to enum string
    @return: dict containing decoded fields
    """
    params = dict()
    params['side'] = pb2.OrderSide.Name(order.side)
    params['status'] = pb2.OrderStatus.Name(order.status)
    params['type'] = pb2.OrderType.Name(order.type)
    params['time_in_force'] = pb2.TimeInForce.Name(order.time_in_force)
    return params


def cleanupAutotraderPorts():
    """Cleans up Autotrader ports"""
    pparser = ProtoParser()
    ports = [int(msg['Port']) for msg in pparser.message_dict.values()]
    for proc in psutil.process_iter():
        name = proc.name()
        connections = proc.connections(kind='inet')
        status = [s.status for s in connections]
        for conn in connections:
            if conn.laddr.port in ports:
                print("Killing PID {}".format(proc.pid))
                proc.kill()
                continue


if __name__ == "__main__":
    import time

    t = time.time()
    pp = ProtoParser()
    print(time.time() - t)
