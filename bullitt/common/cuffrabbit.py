#!/usr/bin/env python
'''
Created on Oct 26, 2011

@author: mmabey
'''


# Library imports
import json
import threading
import urllib

# Third-party libraries
from pika.adapters import SelectConnection, BlockingConnection
import pika
import pika.spec as spec

# Constants
EXCHANGE_TYPES = ('direct', 'topic', 'headers', 'fanout')
DEBUG = False
INFO = False


class RabbitObj(object):
    '''
    This is the class inherited by all other objects that work with RabbitMQ
    within CUFF.
    '''


    def __init__(self,
                 host='localhost',
                 port=spec.PORT,
                 virtual_host='/',
                 credentials=None,
                 channel_max=0,
                 frame_max=spec.FRAME_MAX_SIZE,
                 heartbeat=False):
        '''
        Initializes all the connection parameters and stores them in a 
        ConnectionParameters object at self.conn_params.
        
        Parameters:
        - host: Hostname or IP Address to connect to, defaults to localhost.
        - port: TCP port to connect to, defaults to 5672
        - virtual_host: RabbitMQ virtual host to use, defaults to /
        - credentials: A instance of a credentials class to authenticate with.
          Defaults to PlainCredentials for the guest user.
        - channel_max: Maximum number of channels to allow, defaults to 0 for 
         None
        - frame_max: The maximum byte size for an AMQP frame. Defaults to 
         131072
        - heartbeat: Turn heartbeat checking on or off. Defaults to False.
        '''

        # Send the values to a ConnectionParameters object, which has built-in
        # type-checking
        if isinstance(host, unicode): host = str(host)
        self.conn_params = pika.ConnectionParameters(host, port, virtual_host,
                                                    credentials, channel_max,
                                                    frame_max, heartbeat)
        self.connection = None
        self.channel = None
        self.init_callback = None
        self.exchange = None
        self.routing_key = None
        self.queue_created = False
        self.queue_name = None
        self.no_ack = False
        self._debug_prefix = ""

        # The binding key is essentially the same thing as a routing key, only it is
        # used by receivers instead of senders. We set it to None here so that we can
        # detect on the fly if we have executed a queue_bind() for the binding key
        # before we attempt to receive messages. This is also how we prevent senders
        # from performing a queue_bind(), since they don't generally do that.
        self.binding_key = None


    def init_connection(self, callback, queue_name='', exchange='',
                        exchange_type='topic', routing_key="#", blocking=False,
                        user_id=None):
        '''
        Handles all connection, channel, etc. issues involved in fully 
        connecting to the RabbitMQ server. The function 'callback' is stored
        and called once all steps have successfully completed. Using this 
        method forces all communication to go through the queue specified by 
        'queue'.
        '''
        self.exchange = str(exchange)
        self.init_callback = callback
        if user_id != None:
            self.user_id = str(user_id)

        if exchange_type not in EXCHANGE_TYPES:
            raise ValueError("Exchange type must be one of: %s" % \
                             str(EXCHANGE_TYPES))
        else:
            self.ex_type = str(exchange_type)

        if isinstance(queue_name, unicode):
            queue_name = str(queue_name)
        elif not isinstance(queue_name, str):
            raise TypeError("Queue must be a str, got %s instead" % \
                            str(type(queue_name)))
        self.queue_name = queue_name

        if isinstance(routing_key, unicode):
            routing_key = str(routing_key)
        elif not isinstance(routing_key, str):
            raise TypeError("Routing key must be a str, got %s instead" % \
                            str(type(queue_name)))
        self.routing_key = routing_key

        if DEBUG: print self._debug_prefix + "About to start connection...",

        # From here, determine how to initialize the connection further by 
        # what type of exchange was requested.
        if self.ex_type == "topic":
            self._init_topic_conn()
        #elif self.ex_type == "direct":
        #    self._init_direct_conn()
        elif not blocking:
            self._init_topic_conn()
        else:
            raise NotImplementedError("Only 'topic' and 'direct' exchange " \
                                      "types are currently supported.")


    def _init_topic_conn(self):
        self.connection = SelectConnection(self.conn_params, self.on_connected)
        try:
            # Loop so we can communicate with RabbitMQ
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            # Gracefully close the connection
            self.connection.close()
            # Loop until we're fully closed, will stop on its own
            self.connection.ioloop.start()
        else:
            if DEBUG: print "Connection attempt rejected. Goodbye."


    def _init_direct_conn(self):
        blocking = True
        self.connection = BlockingConnection(self.conn_params)
        self.on_connected(self.connection, blocking)
        self.channel = self.connection.channel()
        self.on_channel_open(self.channel)


    def on_connected(self, connection, blocking=False):
        '''
        '''
        if DEBUG:
            print self._debug_prefix + "Connected\n  Host: %s\n  " \
                  "Exchange: %s\n" % (self.conn_params.host, self.exchange) + \
                  self._debug_prefix + "Creating channel...",
        # These should always be the same, but just in case...
        if self.connection is not connection:
            # Adopt the new connection object
            self.connection = connection
        if not blocking:
            self.connection.channel(on_open_callback=self.on_channel_open)


    def on_channel_open(self, new_channel):
        '''
        '''
        if DEBUG:
            print "Created\n" + self._debug_prefix + \
                  "Declaring %s exchange: '%s'" % (self.ex_type, self.exchange)
        self.channel = new_channel
        self.channel.exchange_declare(exchange=self.exchange,
                                      durable=True,
                                      type=self.ex_type,
                                      callback=self.init_callback)


    #TODO: Mike, I added some correlation_id stuff here.
    #      Using this TODO as a marker so you can find it
    def send_message(self, body, routing_key=None, correlation_id=None):
        '''
        Sends a message to the exchange on the server to which a connection 
        has already been established. Both parameters 'body' and 'routing_key' 
        must be of type str, or a TypeError will be raised. If no routing_key
        is specified, it defaults to the routing key value specified at 
        declaration.
        '''
        if type(body) != str:
            raise TypeError("Parameter 'body' must be of type 'str', got " \
                            "'%s' instead." % type(body))
        if routing_key == None:
            routing_key = self.routing_key
        if type(routing_key) != str:
            raise TypeError("Parameter 'routing_key' must be of type 'str', " \
                            "got '%s' instead." % type(routing_key))

        #if INFO: print "\n" + self._debug_prefix + "Sending message on %s : %s : %s" % (self.exchange, self.queue_name, routing_key)
        # TODO: In the following, create a means of catching "unroutable" messages (necessary because the 'mandatory'
        # flag is set)
        props = pika.BasicProperties(delivery_mode=2, # Persistent messages
                                     user_id=self.user_id,
                                     correlation_id=correlation_id
                                     )
        if DEBUG: 
            print "[x] Sending message to %s" % (routing_key)
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=routing_key,
                                   body=body,
                                   mandatory=True,
                                   properties=props,
                                   )


    def create_queue(self, durable=True, exclusive=False, name=None,
                     callback=None):
        '''
        Typically don't change the default parameters.
        '''
        if not name:
            name = self.queue_name
        if DEBUG: print self._debug_prefix + "Creating queue '%s'..." % name
        self.queue_created = True
        self.channel.queue_declare(durable=durable, exclusive=exclusive,
                                   queue=name, callback=callback)


    def bind_routing_key(self, frame):
        if DEBUG:
            print self._debug_prefix + "Binding queue '%s' to key '%s'" % \
                  (self.queue_name, self.routing_key)
        self.binding_key = self.routing_key # Signals that we've performed the queue binding
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name,
                                routing_key=self.binding_key,
                                callback=self._rcv())


    def receive_message(self, callback=None, no_ack=False, frame=None):
        '''
        Retrieves a message from the channel and queue specified in earlier 
        initialization code. The callback function defaults to 
        self.process_msg. Only under rare circumstances should no_ack be set 
        to True, since this will not inform RabbitMQ that completed tasks can 
        be removed from the queue.
        '''

        if DEBUG: print self._debug_prefix + "Preparing to receive"

        # Set the callback function for processing received messages
        if callback == None:
            self.rcv_callback = self.process_msg
        else:
            self.rcv_callback = callback
        self.no_ack = no_ack

        # Check the queue, declare if necessary
        if not self.queue_created:
            self.create_queue(callback=self.bind_routing_key)

        # Check the binding key, bind the channel if necessary
        elif self.binding_key == None:
            if frame == None:
                raise TypeError("Parameter 'frame' must not be None if the " \
                                "routing key has not yet been bound.")
            self.bind_routing_key(frame) # self._rcv() is specified as the callback in this method

        else:
            self._rcv()


    def _rcv(self):
        self.channel.basic_qos(prefetch_count=1)
        if DEBUG: print self._debug_prefix + "Consuming..."
        self.channel.basic_consume(consumer_callback=self.rcv_callback,
                                   queue=self.queue_name, no_ack=self.no_ack)


    def ack(self, tag):
        self.channel.basic_ack(delivery_tag=tag)


    def reject(self, tag):
        self.channel.basic_reject(delivery_tag=tag)


    def process_msg(self, channel, method, header, body):
        raise NotImplementedError
