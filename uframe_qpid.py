#!/usr/bin/env python
"""
API v1.0 Alerts and Alarms interface for Uframe QPID message broker.
"""
# import optparse
from qpid.messaging import *
# from qpid.util import URL
from qpid.log import enable, DEBUG, WARN

import os
import yaml
# import datetime as dt
from os.path import exists


class Configuration:
    """Open the settings yml, get configuration settings and populate values."""
    qpid_timeout = None
    qpid_broker = None
    qpid_username = None
    qpid_password = None
    qpid_broker_port = None
    qpid_topic = None
    qpid_exchange = None
    qpid_address = None
    qpid_verbose = False
    qpid_forever = None
    qpid_format = "%(M)s"

    def __init__(self):

        filename = "config.yml"
        root = 'COMMON'
        app_root = os.path.dirname(os.path.abspath(__file__))   # refers to application_top
        config_file = os.path.join(app_root, filename)
        # settings = None
        try:
            if exists(config_file):
                stream = open(config_file)
                settings = yaml.load(stream)
                stream.close()
            else:
                raise IOError('No %s configuration file exists!' % config_file)
        except IOError as ioerr:
            # print 'IOError: %s' % err.message
            raise Exception(ioerr.message)

        self.qpid_timeout = settings[root]['UFRAME_QPID_TIMEOUT']
        self.qpid_broker = settings[root]['UFRAME_QPID_BROKER']
        self.qpid_username = settings[root]['UFRAME_QPID_USERNAME']
        self.qpid_password = settings[root]['UFRAME_QPID_PASSWORD']
        self.qpid_broker_port = settings[root]['UFRAME_QPID_BROKER_PORT']
        self.qpid_topic = settings[root]['UFRAME_QPID_TOPIC']
        self.qpid_exchange = settings[root]['UFRAME_QPID_EXCHANGE']
        self.qpid_reconnect = settings[root]['UFRAME_QPID_RECONNECT']
        self.qpid_reconnect_interval = settings[root]['UFRAME_QPID_RECONNECT_INTERVAL']
        self.qpid_reconnect_limit = settings[root]['UFRAME_QPID_RECONNECT_LIMIT']
        self.qpid_verbose = settings[root]['UFRAME_QPID_VERBOSE']
        self.qpid_forever = settings[root]['UFRAME_QPID_FOREVER']

        if not self.qpid_broker or self.qpid_broker is None:
            error_message = 'Configuration value for UFRAME_QPID_BROKER is empty or None.'
            # print '\n message: ', message
            raise Exception(error_message)

        if not self.qpid_broker_port or self.qpid_broker_port is None:
            error_message = 'Configuration value for UFRAME_QPID_BROKER_PORT is empty or None.'
            # print '\n message: ', message
            raise Exception(error_message)

        if not self.qpid_topic or self.qpid_topic is None:
            error_message = 'Configuration value for UFRAME_QPID_TOPIC is empty or None.'
            # print '\n message: ', message
            raise Exception(error_message)

        if not self.qpid_exchange or self.qpid_exchange is None:
            error_message = 'Configuration value for UFRAME_QPID_EXCHANGE is empty or None.'
            # print '\n message: ', message
            raise Exception(error_message)

        self.qpid_address = "/".join([self.qpid_exchange, self.qpid_topic])

    def settings(self):
        """ Display all configuration file settings. Debug - disable for production."""
        print '\n self.qpid_timeout: ', self.qpid_timeout
        print '\n self.qpid_broker: ', self.qpid_broker
        print '\n self.qpid_username: ', self.qpid_username
        print '\n self.qpid_password: ', self.qpid_password
        print '\n self.qpid_broker_port: ', self.qpid_broker_port
        print '\n self.qpid_topic: ', self.qpid_topic
        print '\n self.qpid_exchange: ', self.qpid_exchange
        print '\n self.qpid_address: ', self.qpid_address
        print '\n self.qpid_reconnect: ', self.qpid_reconnect
        print '\n self.qpid_reconnect_interval: ', self.qpid_reconnect_interval
        print '\n self.qpid_reconnect_limit: ', self.qpid_reconnect_limit
        print '\n self.qpid_verbose: ', self.qpid_verbose
        print '\n self.qpid_forever: ', self.qpid_forever
        print '\n self.qpid_format: ', self.qpid_format
        print '\n '


class Formatter:
    def __init__(self, queue_message):
        self.message = queue_message
        self.environ = {"M": self.message,
                        "P": self.message.properties,
                        "C": self.message.content}

    def __getitem__(self, st):
        return eval(st, self.environ)


conn = None
try:
    print '\n Starting...'

    # Read configuration file for settings
    config = Configuration()
    config.settings()

    # Enable DEBUG or WARN and timeout
    if config.qpid_verbose:
        enable("qpid", DEBUG)
    else:
        enable("qpid", WARN)
    if config.qpid_forever is True:
        timeout = None
    else:
        timeout = config.qpid_timeout

    # Configure connection string for broker; connect to broker and start session
    conn_string = config.qpid_broker
    if config.qpid_username:
        conn_string = '{0}/{1}@{2}:{3}'.format(config.qpid_username, config.qpid_password,
                                               config.qpid_broker, config.qpid_broker_port)
    else:
        conn_string = '{0}:{1}'.format(config.qpid_broker, 5672)
    try:
        conn = Connection(conn_string,
                          reconnect=config.qpid_reconnect,
                          reconnect_interval=config.qpid_reconnect_interval,
                          reconnect_limit=config.qpid_reconnect_limit)
    except Exception, err:
        message = 'Failed to connect to qpid broker. Error: %s', err.message
        print '\n message: ', message
        raise Exception(message)

    conn.open()
    session = conn.session()

    # Configure session receiver and determine if messages are available
    '''
    Reference:
    https://qpid.apache.org/releases/qpid-0.14/books/Programming-In-Apache-Qpid/html/ch02s04.html#table-node-properties

    type = {queue | topic}
    mode = {browse | consume}
      address_rcv = addr  + "; { node: { type: queue }, assert: never , create: never, mode: " + "browse" + " }"
      rcv = ssn.receiver(source=address_rcv)
    '''
    # modified_address = config.qpid_address +
    # "; { node: { type: topic }, assert: always, create: never, mode: " + "browse" + " }"
    receiver = session.receiver(source=config.qpid_address)  # modified_address)
    available_messages = 0
    try:
        available_messages = receiver.available()
        print '\n\nAvailable messages: %s\n' % str(receiver.available())
    except Exception as err:
        message = 'Error when retrieving number of messages available for receiver. (%s)' % err.message
        print '\n message: ', message
        raise Exception(message)

    # Process available messages one at a time, sending an acknowledge after each
    count = 0
    while available_messages > 0 or count < available_messages:
        try:
            msg = receiver.fetch(timeout=config.qpid_timeout)
            if config.qpid_verbose:
                print config.qpid_format % Formatter(msg)
            count += 1
            session.acknowledge()
        except Empty:
            print "\n Exception - Empty"
            break

except ReceiverError as recv_err:
    print recv_err

except KeyboardInterrupt:
    pass

except Exception as ex:
    message = 'General exception: ', ex.message
    print '\n message: ', message
    pass

finally:
    if conn:
        conn.close()
