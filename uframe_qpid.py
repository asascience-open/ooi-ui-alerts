#!/usr/bin/env python
'''
API v1.0 Alerts and Alarms interface for Uframe qpid message broker.
'''
import optparse
from qpid.messaging import *
#from qpid.util import URL
from qpid.log import enable, DEBUG, WARN

import os
from os.path import exists
import yaml
import datetime as dt
import time
import requests
import json
from dateutil.parser import parse as parse_date
import pytz

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# class Configuration
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Configuration:
    """Open the settings yml, get configuration settings and populate values."""
    qpid_timeout = None
    qpid_broker = None
    qpid_username = None
    qpid_password = None
    qpid_broker_port = None
    qpid_topic = None
    qpid_exchange = None
    qpid_reconnect = None
    qpid_reconnect_interval = None
    qpid_reconnect_timeout = None
    qpid_reconnect_limit = None
    qpid_verbose = False
    qpid_format = "%(M)s"
    qpid_fetch_interval = None
    qpid_address = None
    ooi_timeout = None
    ooi_timeout_read = None

    def __init__(self):

        filename = "config.yml"
        root = 'COMMON'
        APP_ROOT = os.path.dirname(os.path.abspath(__file__))   # refers to application_top
        config_file = os.path.join(APP_ROOT, filename)
        settings = None
        try:
            if exists(config_file):
                stream = open(config_file)
                settings = yaml.load(stream)
                stream.close()
            else:
                raise IOError('No %s configuration file exists!' % config_file)
        except IOError, err:
            #print 'IOError: %s' % err.message
            raise Exception(err.message)

        self.qpid_timeout = settings[root]['UFRAME_QPID_TIMEOUT']
        self.qpid_broker = settings[root]['UFRAME_QPID_BROKER']
        self.qpid_username = settings[root]['UFRAME_QPID_USERNAME']
        self.qpid_password = settings[root]['UFRAME_QPID_PASSWORD']
        self.qpid_broker_port = settings[root]['UFRAME_QPID_BROKER_PORT']
        self.qpid_topic = settings[root]['UFRAME_QPID_TOPIC']
        self.qpid_exchange = settings[root]['UFRAME_QPID_EXCHANGE']
        self.qpid_reconnect = settings[root]['UFRAME_QPID_RECONNECT']
        self.qpid_reconnect_interval = settings[root]['UFRAME_QPID_RECONNECT_INTERVAL']
        self.qpid_reconnect_timeout = settings[root]['UFRAME_QPID_RECONNECT_TIMEOUT']
        self.qpid_reconnect_limit = settings[root]['UFRAME_QPID_RECONNECT_LIMIT']
        self.qpid_verbose = settings[root]['UFRAME_QPID_VERBOSE']
        self.qpid_fetch_interval = settings[root]['UFRAME_QPID_FETCH_INTERVAL']
        self.host = settings[root]['HOST']
        self.port = settings[root]['PORT']
        self.ooi_timeout = settings[root]['OOI_TIMEOUT']
        self.ooi_timeout_read = settings[root]['OOI_TIMEOUT_READ']

        if not self.qpid_broker or self.qpid_broker is None:
            message = 'Configuration value for UFRAME_QPID_BROKER is empty or None.'
            #print '\n message: ', message
            raise Exception(message)

        if not self.qpid_broker_port or self.qpid_broker_port is None:
            message = 'Configuration value for UFRAME_QPID_BROKER_PORT is empty or None.'
            #print '\n message: ', message
            raise Exception(message)

        if not self.qpid_topic or self.qpid_topic is None:
            message = 'Configuration value for UFRAME_QPID_TOPIC is empty or None.'
            #print '\n message: ', message
            raise Exception(message)

        if not self.qpid_exchange or self.qpid_exchange is None:
            message = 'Configuration value for UFRAME_QPID_EXCHANGE is empty or None.'
            #print '\n message: ', message
            raise Exception(message)

        self.qpid_address = "/".join([self.qpid_exchange, self.qpid_topic])

    def settings(self):
        """ Display all configuration file settings. Debug - disable for production."""
        print '\n qpid_timeout: ', self.qpid_timeout
        print '\n qpid_username: ', self.qpid_username
        print '\n qpid_password: ', self.qpid_password
        print '\n qpid_broker: ', self.qpid_broker
        print '\n qpid_broker_port: ', self.qpid_broker_port
        print '\n qpid_topic: ', self.qpid_topic
        print '\n qpid_exchange: ', self.qpid_exchange
        print '\n qpid_address: ', self.qpid_address
        print '\n qpid_reconnect: ', self.qpid_reconnect
        print '\n qpid_reconnect_interval: ', self.qpid_reconnect_interval
        print '\n qpid_reconnect_timeout: ', self.qpid_reconnect_timeout
        print '\n qpid_reconnect_limit: ', self.qpid_reconnect_limit
        print '\n qpid_verbose: ', self.qpid_verbose
        print '\n qpid_format: ', self.qpid_format
        print '\n qpid_fetch_interval: ', self.qpid_fetch_interval
        print '\n host: ', self.host
        print '\n ooi_timeout: ', self.ooi_timeout
        print '\n ooi_timeout_read: ', self.ooi_timeout_read
        print '\n '

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# class [Message] Formatter
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Formatter:

  def __init__(self, message):
    self.message = message
    self.environ = {"M": self.message,
                    "P": self.message.properties,
                    "C": self.message.content}

  def __getitem__(self, st):
    return eval(st, self.environ)

#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
# private helper methods
#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
def display_all_message_contents(message):
    """
    Development tool for examining uframe qpid message contents; non-production use only!
    """
    print '\n ------ message.id: ', message.id
    print '\n ------ message.user_id: ', message.user_id
    print '\n ------ message.priority: ', message.priority
    print '\n ------ message.ttl: ', message.ttl
    print '\n ------ message.durable: ', message.durable
    print '\n ------ message.properties: ', message.properties
    print '\n ------ message.content: ', message.content
    return

def persist_system_event(message, url, timeout, timeout_read):
    """
    Process uframe message and persist as SystemEvent

    Targeted Input message.content format:
        Message 00001 = {
            "severity":-1,
            "attributes":{
                "subsite":"CE01ISSP",
                "sensor":"01-CTDPFJ123",
                "id":"6ccade2f-897f-4ad8-aaab-bb5bb9c128ad",
                "node":"SP001",
                "time":3.645963123E9,
                "filterId":28,            << Note added filterId attribute
                "eventId":6140270,
                "method":"telemetered",
                "deployment":1,
                "severity":-1
            },
            "payload":null,
            "messageText":"Parameter [temperature:9.980400] v > 8.000000"
        }

    Current (sample) input message format:
        Message(id=UUID('673c2bd9-7d37-3f8a-8733-3eb30a275590'),
        user_id='guest', priority=4, ttl=60.0, durable=True,
        properties={u'node': u'XX099', u'eventId': 10303, 'x-amqp-0-10.routing-key': u'alertalarm.msg',
        u'severity': -2, 'x-amqp-0-10.timestamp': timestamp(1436642340915.0),
        u'sensor': u'01-CTDPFJ999', u'deployment': 1, u'method': u'telemetered', u'time': 3607761508.727,
        u'subsite': u'CE01ISSP', u'id': u'23f9b763-263a-4328-b062-1273f93932a7'},
        content=u'{"messageText":"Parameter [temperature:11.261400] v > 10.000000",
        "severity":-2,"attributes":{"subsite":"CE01ISSP","sensor":"01-CTDPFJ999",
        "id":"23f9b763-263a-4328-b062-1273f93932a7","node":"XX099",
        "time":3.607761508727E9,"eventId":10303,"method":"telemetered",
        "deployment":1,"severity":-2},"payload":null}')

    message.content:
    {
        u'attributes':
            {
                u'node': u'XX099',
                u'eventId': 41927,
                u'severity': -2,
                u'subsite': u'CE01ISSP',
                u'deployment': 1,
                u'method': u'telemetered',
                u'time': 3607761441.727,
                u'sensor': u'01-CTDPFJ999',
                u'id': u'3325b1ae-6c0e-434b-a2a3-2552cda914cc'
            },
        u'messageText': u'Parameter [temperature:10.739700] v > 10.000000',
        u'severity': -2,
        u'payload': None
    }

    Dictionary format for POST /alerts_alarms
    {
        "uframe_filter_id": 2,
        "event_response": "Parameter [temperature:10.739700] v > 10.000000",
        "event_type": "alarm",
        "event_time": 3607761441.727,
        "system_event_definition_id": null,
        "uframe_event_id": 41927
    }

    SystemEvent returned:
        {
          "event_response": "Parameter [temperature:11.088800] v > 10.000000",
          "event_time": "Fri, 17 Jul 2015 12:53:33 GMT",
          "event_type": "alarm",
          "id": 599,
          "system_event_definition_id": 2,
          "uframe_event_id": 41895,
          "uframe_filter_id": 2
        }
    """
    debug = False

    # Get values from message for data dictionary
    content = json.loads(str(message.content))

    # todo verify required content is available
    attributes = content['attributes']
    if 'eventId' not in attributes:
        uframe_event_id = -1                        # processing an alert
    else:
        uframe_event_id = attributes['eventId']     # processing an alarm

    if debug: print '\n message.content: ', content
    if 'filterId' in attributes:
        uframe_filter_id = attributes['filterId']                  #todo Change when available
    else:
        uframe_filter_id = 2

    severity = attributes['severity']
    method = attributes['method']
    deployment = attributes['deployment']
    event_type = 'alarm'
    if severity > 0:
        event_type = 'alert'
    event_response_message = content['messageText']
    event_time = attributes['time']             # convert attribute['event_time'] to date in ooi-ui-services

    # Populate dictionary for POST
    event_data = {}
    event_data['uframe_event_id'] = uframe_event_id
    event_data['uframe_filter_id'] = uframe_filter_id
    event_data['system_event_definition_id'] = None
    event_data['event_time'] = event_time
    event_data['event_type'] = event_type
    event_data['event_response'] = event_response_message
    event_data['method'] = method
    event_data['deployment'] = deployment
    new_event = json.dumps(event_data)
    if debug: print '\n event_data: ', event_data
    try:
        # Send request to ooi-ui-services to persist SystemEvent
        response = requests.post(url, timeout=(timeout, timeout_read),  data=new_event) # headers=headers,
        if response.status_code != 201:
            message = 'Error: status code: %s' % str(response.status_code)
            if debug: print '\n uframe_qpid persist exception: message: ', message

        if debug: print '\n SystemEvent: ', response.content
    except Exception as err:
        # todo - investigate this problem (intermittent connection refused by ooi-ui-services)
        print '\n Failed to persist event_data: %r; %s ' % (event_data,  err.message)

    return

'''
def iso_to_timestamp(iso8601):
    pdt = parse_date(iso8601)
    t = (pdt - dt(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
    return t
'''

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Long running service
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
conn = None
try:

    print '\n Starting...'

    # Read configuration file for settings
    config = Configuration()
    config.settings()

    # verify ooi-ui-services are available - if not, abort
    # todo consider security/auth access for 12577
    #headers = get_api_headers('admin', 'test')
    base_url = 'http://' + config.host + ':' + str(config.port)
    test_url = "/".join([ base_url, 'alert_alarm_definition'])
    ooi_timeout = config.ooi_timeout
    ooi_timeout_read = config.ooi_timeout_read
    try:
        response = requests.get(test_url, timeout=(ooi_timeout, ooi_timeout_read))
        if response.status_code != 200:
            message = 'Unable to connect to ooi-ui-services, aborting. status code: %s' % str(response.status_code)
            #print '\n message: ', message
            raise Exception(message)
    except Exception as err:
        message = 'Verify configuration and availability of ooi-ui-services, aborting. Error: %s' % str(err.message)
        #print '\n message: ', message
        raise Exception(message)

    # Enable qpid DEBUG or WARN
    if config.qpid_verbose:
        enable("qpid", DEBUG)
    else:
        enable("qpid", WARN)

    timeout = config.qpid_timeout

    # Configure connection string for broker; connect to broker and start session
    conn_string = config.qpid_broker
    if config.qpid_username:
        conn_string = '{0}/{1}@{2}:{3}'.format(config.qpid_username, config.qpid_password,
                                               config.qpid_broker, config.qpid_broker_port)
    else:
        conn_string = '{0}:{1}'.format(config.qpid_broker, 5672)
    try:
        conn = Connection(conn_string, reconnect=config.qpid_reconnect,
                                       reconnect_interval=config.qpid_reconnect_interval,
                                       reconnect_limit=config.qpid_reconnect_limit,
                                       reconnect_timeout=config.qpid_reconnect_timeout)
    except Exception, err:
        message = 'Failed to connect to qpid broker. Error: %s', err.message
        print '\n message: ', message
        raise Exception(message)

    debug = False
    # Configure connection and session receiver
    conn.open()
    session = conn.session()
    receiver = session.receiver(config.qpid_address)

    # Process available messages one at a time, sending an acknowledge after each.
    url = "/".join([base_url, "alert_alarm"])
    qpid_fetch_interval = config.qpid_fetch_interval
    if qpid_fetch_interval < 1:         # config?
        qpid_fetch_interval = 3         # default value config?
    loop_on = True
    while loop_on == True:
        try:
            msg = receiver.fetch(timeout=config.qpid_timeout)
            if msg is not None:
                if debug:
                    print config.qpid_format % Formatter(msg)
                    print '\n '
                    display_all_message_contents(msg)
                print '\n Persist system event...'
                persist_system_event(msg, url, ooi_timeout, ooi_timeout_read)
                print ' Performing session.acknowledge()'
                session.acknowledge()
        except Empty:
            pass
        except Exception as err:
            print "\n Exception: ", err.message

        time.sleep(qpid_fetch_interval)

except ReceiverError, e:
    print 'ReceiverError: ', e
except KeyboardInterrupt:
    pass
except Exception as err:
    message = 'General exception: ', err.message
    print '\n\n %s \n\n' % str(err.message)
    pass

finally:
    if conn:
        conn.close()

