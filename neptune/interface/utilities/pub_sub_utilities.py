#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilities for creating test Publishers and Subscribers
"""
import threading
import time
import logging
import pytz
import dateutil
from datetime import datetime
from pathlib import Path
import pandas as pd
import pathlib
from google.protobuf.json_format import MessageToDict

from alpaca.interface.utilities import proto_utilities

from alpaca.interface.generated import interface_pb2 as pb2

import alpaca.conf.config


def parse_plf_filestring(filepath: Path) -> tuple:
    """Parse PLF to get type and date

    @param filepath: path to PLF file
    @return: tuple (type of file, date of file)
    """
    _type = str(filepath.stem.split('_')[0])
    _date = dateutil.parser.isoparse(filepath.stem.split('_')[1]).astimezone(pytz.timezone('UTC'))
    return _type, _date


def get_proto_obj(object_type: str):
    """Get proto message object of specified type

    @param object_type: Protobuf message object type string
    @return: Protobuf message object
    """
    try:
        proto_obj = eval("pb2.{}()".format(object_type))
    except Exception as e:
        print("ERROR: Invalid proto interface type - {}\n{}".format(object_type, str(e)))
        raise
    return proto_obj


class TestSubscriber(threading.Thread):
    """
    Generic test subscriber for listening and recording Autotrader interface messages. Each subscriber will
    start its own thread
    """

    def __init__(self, msg: str, host='127.0.0.1',
                 channel='', autostart=False):
        """
        Create a TestSubscriber object
        @param msg: data type of proto message
        @param host: host to connect to (defaults to localhost)
        @param channel: topic to listen to (defaults to all topics)
        @param autostart: run the subscriber upon creation
        """

        # Call Thread constructor
        super(TestSubscriber, self).__init__(name="TestSub-{}:{}".format(host, msg), daemon=True)

        self.logger = logging.getLogger(name="TestSub-{}".format(msg))
        self._msg = msg
        self._host = host
        self._socket = proto_utilities.ProtoParser().get_socket(msg)
        self._proto_obj = get_proto_obj(msg)
        if channel != '':
            self._socket.unsubscribe('')
            self._socket.subscribe(channel)

        # Construct the path where the log file will be written
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        log_name = '{}_{}.plf'.format(msg, timestamp)
        self._log_filepath = alpaca.conf.config.LOG_DIRECTORY / log_name
        self.logger.info("Writing to {}".format(self._log_filepath))

        if autostart:
            self.start()

    def __del__(self):
        self._socket.close()

    def run(self):
        """
        Main loop for TestSubscriber
        """
        print("Listening to {}".format(self._msg))
        fp = None
        while True:
            try:
                # Blocking receive
                topic, msg = self._socket.recv_multipart()

                # Parse message
                self._proto_obj.ParseFromString(msg)
                self.logger.debug("Received {} bytes".format(len(msg)))

                # Open file for writing
                if not fp:
                    fp = open(self._log_filepath, 'wb')

                # Write 2-byte block size and data block to file
                fp.write((len(msg)).to_bytes(2, byteorder='big', signed=False))
                fp.write(msg)
                fp.flush()
            except Exception as e:
                self.logger.warning(e)
                fp.flush()
                fp.close()


class TestPublisher(threading.Thread):
    """
    Generic test publisher for playing back data at approximate receive rate. Takes a *.plf file - only works with 
    Aggregate data currently
    """

    def __init__(self, file: Path, host='127.0.0.1',
                 channel='', autostart=False, rate=1):
        """
        Create a TestPublisher object
        @param file: data type of proto message
        @param host: host to bind to (defaults to localhost)
        @param channel: topic to listen to (defaults to all topics)  [unused]
        @param autostart: run the subscriber upon creation
        @param rate: speed of playback (i.e. 3 -> 3x playback rate)
        """
        _type, _ = parse_plf_filestring(filepath=file)
        super(TestPublisher, self).__init__(name="TestPub-{}:{}".format(host, _type), daemon=True)
        self.logger = logging.getLogger(name="TestPub-{}".format(_type))
        self._file = file
        self._type = _type
        self._host = host
        self._socket = proto_utilities.ProtoParser().get_socket(message=_type, sub=False, bind=True)
        self._count = 0
        self._msgTime = 0
        self._rate = rate

        if autostart:
            self.start()

    def getMessageCount(self) -> int:
        """Return the message count then resets count to 0
        @return: message count
        """
        count = self._count
        self._count = 0
        return count

    def run(self):
        """
        Main loop for TestSubscriber
        """
        proto_obj = get_proto_obj(self._type)
        with open(str(self._file), 'rb') as fp:
            self.logger.info("\nReplaying {}".format(self._file))

            # Read first message
            msg_size = int.from_bytes(bytes=fp.read(2), byteorder='big', signed=False)
            proto_obj.ParseFromString(fp.read(msg_size))
            while True:
                # Create topic string, publish data
                topic = proto_obj.sym
                self._socket.send_multipart([topic.encode(), proto_obj.SerializeToString()])

                # Store previous message time
                t_prev = proto_obj.hdr.utc_time

                # Read next message size
                msg_size = int.from_bytes(bytes=fp.read(2), byteorder='big', signed=False)
                if not msg_size:
                    break

                proto_obj.ParseFromString(fp.read(msg_size))
                t_current = proto_obj.hdr.utc_time

                ts_current = datetime.fromtimestamp(t_current / 100)
                sleep_time = max(0.1, t_current - t_prev) / 1000
                if sleep_time > 2:
                    time.sleep(sleep_time / self._rate)
                self._count += 1
                self._msgTime = ts_current

            self.logger.info("Done replaying.")


class TestDecoder:
    """Decodes the PLF binary files written by TestSubscriber"""

    def __init__(self, folder: str):
        """Constructor
        @param folder: Input folder with plf files
        """
        # Dict to store DataFrames for published messages
        self.folder = folder
        self.files = TestDecoder._get_file_list(Path(folder))

        # Dict to store DataFrames for published messages
        self.data = dict()

        # For each *.plf file, read and parse
        for f in self.files:
            if not f['decoded']:
                TestDecoder._decode_plf(plf_info=f)

    @staticmethod
    def _get_file_list(folder: Path) -> list:
        files = list(pathlib.Path(folder).glob('*.plf'))
        file_l = list()
        for f in files:
            buffer = dict()
            buffer['type'], buffer['date'] = parse_plf_filestring(filepath=f)
            buffer['plf'] = f
            buffer['csv'] = f.with_suffix('.csv')
            buffer['pkl'] = f.with_suffix('.pkl')
            buffer['decoded'] = True if buffer['csv'].exists() else False
            file_l.append(buffer)
        return file_l

    @staticmethod
    def _decode_plf(plf_info: dict):
        proto_info = proto_utilities.ProtoParser()
        dtype_info = proto_info.lookup(plf_info['type'])

        # Create corresponding proto object
        proto_obj = get_proto_obj(plf_info['type'])

        msg_buffer = list()
        with open(plf_info['plf'], 'rb') as fp:
            print("\nDecoding PLF file: {}".format(plf_info['csv']))
            while True:
                msg_size = int.from_bytes(bytes=fp.read(2), byteorder='big', signed=False)
                if not msg_size:
                    break
                try:
                    proto_obj.ParseFromString(fp.read(msg_size))
                    d = MessageToDict(proto_obj, including_default_value_fields=True)
                    d['hdrTime'] = d['hdr']['utc_time']
                    del d['hdr']
                    msg_buffer.append(d)
                except Exception as e:
                    print("ERROR: Unable to parse message for {}\n{}".format(plf_info['plf'], str(e)))

        # Convert JSON file to DataFrame
        df = pd.DataFrame(msg_buffer)

        # Make readable timestamp
        try:
            if dtype_info is not None:
                for k, v in dtype_info['Fields'].items():
                    if "POSIX_MS" in v['Summary']:
                        df[k] = pd.to_datetime(df[k], unit='ms')
            df['hdrTime'] = pd.to_datetime(df['hdrTime'], unit='ms')
        except Exception as e:
            pass

        # Save as CSV
        print("Writing CSV file: {}".format(plf_info['csv']))
        df.to_csv(plf_info['csv'], header=True, index=False)


if __name__ == "__main__":
    alpaca.conf.config.configure_logging()

    td = TestDecoder(alpaca.conf.config.LOG_DIRECTORY)

    print()
    ts = TestSubscriber(msg="Order", autostart=True)

    while True:
        time.sleep(10)
