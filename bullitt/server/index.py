#!/usr/bin/env python
'''
Created on Apr 5, 2012

@author: mmabey
'''

# Library imports
import json
import os

# Third-party libraries

# Local imports
from bullitt.common.cuffrabbit import RabbitObj
from db import ServerBiz

# Constants
DEBUG = False
INFO = True



class Server(RabbitObj):
    '''
    The main command and control center for the Bullitt Index Server.
    
    This class functions primarily as a switching board of sorts by receiving
    messages from clients, using the business logic class, db.ServerBiz, and
    responding appropriately.  The server has been designed to be single-
    threaded to help ensure transactions are atomic.  While a multi-threaded
    approach may be more time efficient, it may also introduce consistency
    issues with the database which is far less desirable than less optimal
    performance.
    
    Post explaining the seemingly arbitrary slice size:
    http://www.unitethecows.com/p2p-general-discussion/30807-rodi-anonymous-p2p.html#post203703
    '''


    def __init__(self):
        '''
        Constructor
        '''
        # Get configuration settings
        p = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(p, 'server_config.json')) as fin:
            config = json.load(fin)
        p = os.path.realpath(os.path.join(p, '..', 'common'))
        with open(os.path.join(p, 'gen_config.json')) as fin:
            genconfig = json.load(fin)
        host = config['rabbit_server']
        queue = genconfig['op_queue']
        
        # Operation parameters
        self.ops = dict(add_file=('id', 'name', 'bytes', 'sha1'),
                        del_file=('id', 'name', 'sha1'),
                        get_peers=('id',),
                        grant=('id', 'client', 'read', 'write'),
                        mod_file=('id', 'name', 'bytes', 'sha1', 'prev_sha1'),
                        query_rights=('id',),
                        request_file=('id', 'sha1'),
                        revoke=('id', 'client', 'read', 'write'),
                        update_complete=('id', 'sha1'))
        
        # Create DB biz object
        self.biz = ServerBiz()
        
        # Initialize connection parameters
        RabbitObj.__init__(self, dict(host=host))
        
        # Connect to MQ server. Should be last thing in this method.
        self.init_connection(callback=self.main, queue_name=queue,
                             exchange_type='direct')
    
    
    def main(self):
        '''
        '''
        # Start listening to the queue
        self.receive_message(callback=self.process_msg)
        
        # Do anything else that should be asynchronous to listening for messages
    
    
    def process_msg(self, ch, method, props, body):
        '''
        If the message from the client is valid, perform the requested action.
        
        Since this method is specified as the callback whenever a message is
        received from a client, 
        '''
        # Extract client's ID and check it is valid
        client_id = props.user_id
        
        # Parse message
        job_data = json.loads(body)
        
        # Ensure the client is valid and the message is well-formed
        if not self.biz.client_exists(client_id) \
               or not job_data.has_key('msg_type') \
               or not job_data.has_key('params') \
               or not job_data['msg_type'] in self.ops \
               or not self._check_param_keys(job_data['params'],
                                             job_data['msg_type']):
            #TODO: Do something? At least reject the message...
            return
        
        action = job_data['msg_type']
        params = job_data['params']
        
        # Perform requested action
        if action in ('add_file', 'mod_file', 'del_file', 'grant', 'revoke',
                      'update_complete'):
            # These operations don't need any further processing past the
            # business logic
            self.biz.__dict__[action](params, client_id)
        else:
            self.__dict__['_' + action](params, client_id)
        
        # Acknowledge message
        self.ack(method.delivery_tag)
    
    
    def _check_param_keys(self, params, action):
        '''
        Make sure the client's message has all necessary information.
        '''
        for k in self.ops[action]:
            if k not in params or params[k].strip() == '' or params[k] == None:
                # Ignore the request
                return False
        return True
    
    
    def _get_peers(self, params, client_id):
        '''
        '''
    
    
    def _query_rights(self, params, client_id):
        ret = self.biz.get_file_owner(params['id'])
        if client_id == ret:
            # Client is file owner
            pass
    
    
    def _request_file(self, params, client_id):
        pass















