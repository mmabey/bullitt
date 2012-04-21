#!/usr/bin/env python
'''
Created on Apr 5, 2012

@author: mmabey
'''

# Library imports
import json
from math import ceil
import os
import Queue
import threading

# Third-party libraries

# Local imports
from bullitt.common import cuffrabbit
from db import ServerBiz
from pika.credentials import PlainCredentials

# Constants
DEBUG = False
INFO = False
VERBOSE = False



class Server(object):
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
        config  # Shut up the stupid eclipse warning
        p = os.path.realpath(os.path.join(p, '..', 'common'))
        with open(os.path.join(p, 'gen_config.json')) as fin:
            genconfig = json.load(fin)
        host = genconfig['rabbit_server']
        exchange = genconfig['server_exchange']
        queue = genconfig['op_queue']
        self.slice_size = genconfig['slice_size']
        
        # Create DB biz object
        self.biz = ServerBiz()
        
        # Create Listener object
        self.signal_queue = Queue.Queue()
        self.listener = _Listener(self, host, queue, self.slice_size)
        self.listener.exchange = exchange
        self.listener.start()
    
    
    def add_client(self, client_id, ipaddr, pub_key):
        '''
        Add a client to the system with the given ID, IP address, and pub key.
        '''
        self.biz.add_client(client_id, ipaddr, pub_key)
    
    
    def get_clients(self):
        '''
        '''
        return self.biz.get_clients()
    
    
    def del_client(self, client_id=None, ipaddr=None):
        '''
        Given either a client ID or a client IP address, delete the client.
        
        Returns False if unsuccessful.
        '''
        if not client_id:
            if not ipaddr:
                return False
            info = self.biz.client_lookup(ipaddr=ipaddr)
            if not info:
                if DEBUG:
                    print "Could not find a client with IP address %s" % ipaddr
                return
            client_id = info['user_id']
        return self.biz.del_client(client_id)



class _Listener(cuffrabbit.RabbitObj, threading.Thread):
    '''
    '''
    
    def __init__(self, parent, host, queue, slice_size):
        '''
        '''
        # Initialize thread
        threading.Thread.__init__(self)
        self.daemon = True
        
        # Initialize connection parameters
        self.user_id = 'server'
        creds = PlainCredentials(self.user_id, 'server')
        cuffrabbit.RabbitObj.__init__(self, **dict(host=host,
                                                   credentials=creds))
        self._queue_name = queue
        self.parent = parent
        self.biz = parent.biz
        self.slice_size = slice_size
        
        # Operation parameters
        self.ops = dict(add_file=('id', 'name', 'bytes', 'sha1'),
                        client_lookup=(), # Do param verification differently
                        del_file=('id', 'name', 'sha1'),
                        get_peers=('id',),
                        grant=('id', 'client', 'read', 'write'),
                        list_files=(), # No parameters needed
                        mod_file=('id', 'name', 'bytes', 'sha1', 'prev_sha1'),
                        query_rights=('id',),
                        request_file=('id', 'sha1'),
                        revoke=('id', 'client', 'read', 'write'),
                        version_downloaded=('id', 'sha1'),)
    
    
    def run(self):
        # Connect to MQ server. Should be last thing in this method.
        if VERBOSE: print "[i] Initiating connection to server..."
        self.init_connection(callback=self.main, queue_name=self._queue_name,
                             exchange=self.exchange, exchange_type='direct',
                             routing_key=self._queue_name)
    
    
    def main(self, stupid):
        '''
        '''
        # Start listening to the queue
        if DEBUG or VERBOSE:
            print "[i] Starting to listen on %s" % self._queue_name
        
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
        if VERBOSE: print "[.] Received message from %s" % client_id
        
        # Parse message
        job_data = json.loads(body)
        
        # Ensure the client is valid and the message is well-formed
        if not self.biz.client_exists(client_id) \
               or not job_data.has_key('msg_type') \
               or not job_data.has_key('params') \
               or not job_data['msg_type'] in self.ops \
               or not self._check_param_keys(job_data['params'],
                                             job_data['msg_type']):
            #not necessary for this version... TODO Do something? At least reject the message...
            self.ack(method.delivery_tag) # Oh well... we'll just acknowledge it
            return
        
        action = job_data['msg_type']
        params = job_data['params']
        
        if VERBOSE: print "%sAction is '%s'" % ((' ' * 7), action)
        
        # Perform requested action
        if action in ('add_file', 'grant', 'revoke', 'update_complete'):
            # These operations don't need any further processing past the
            # business logic
            getattr(self.biz, action)(params, client_id)
        else:
            if ('_' + action) not in dir(self):
                raise AttributeError('Method "%s" not found in class Server' % \
                                     action)
            getattr(self, '_' + action)(params, client_id)
        
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
    
    
    def _client_lookup(self, params, client_id):
        '''
        '''
        ret = self.biz.get_clients()
        body = dict(msg_type='client_list',
                    params=dict(clients=ret))
        self.send_message(body=json.dumps(body), routing_key=client_id)
    
    
    def _del_file(self, params, client_id):
        '''
        Delete a file if client owns it, then notify its peers of the deletion.
        '''
        file_id = params['file_id']
        sha1 = params['sha1']
        if  sha1 != self.biz.get_file_version(file_id):
            #not necessary for this version... TODO ? Notify client that delete failed because the specified version is out of date
            return
        elif not self.biz.del_file(params, client_id):
            #not necessary for this version... TODO ? Notify client that delete failed for some other reason
            return
        
        # Notify peers to delete the file
        peers = self.biz.get_file_peers(file_id, client_id, 'all',
                                        get_pub_key=False)
        if peers:
            # Only bother to create the message if there are peers on the file
            body = dict(msg_type="file_deleted",
                        params=dict(file_id=file_id, sha1=sha1))
            for p in peers:
                self.send_message(body=json.dumps(body),
                                  routing_key=p['user_id'])
    
    
    def _get_peers(self, params, client_id):
        '''
        Return the list of other clients that have access to a file.
        '''
        file_id = params['id']
        sha1 = None
        if 'sha1' in params: sha1 = params['sha1']
        ret = self.biz.get_file_peers(file_id, client_id, sha1)
        body = dict(msg_type='peers_list',
                    params=dict(peers=ret))
        self.send_message(body=json.dumps(body), routing_key=client_id)
    
    
    def _list_files(self, params, client_id):
        '''
        Return the list of files to which the client has access.
        '''
        ret = self.biz.get_client_files(client_id)
        body = dict(msg_type="files_list",
                    params=dict(files=ret))
        self.send_message(body=json.dumps(body), routing_key=client_id)
    
    
    def _mod_file(self, params, client_id):
        '''
        '''
        ret = self.biz.mod_file(params, client_id)
        if not bool(ret):
            # Modification failed. Don't do anything else.
            #TODO: Make sure the above is correct
            return
        
        file_id = params['id']
        sha1 = params['sha1']
        
        # Notify peers to get the updated version
        peers = self.biz.get_file_peers(file_id, client_id, 'all',
                                        get_pub_key=False)
        if peers:
            # Only bother to create the message if there are peers on the file
            body = dict(msg_type="file_deleted",
                        params=dict(file_id=file_id, sha1=sha1))
            for p in peers:
                body = dict(msg_type='file_modded',
                            params=dict(ret_val=ret))
                #TODO: Make sure the above complies with message structure
                self.send_message(body=json.dumps(body), routing_key=p)
    
    
    def _query_rights(self, params, client_id):
        '''
        '''
        file_id = params['id']
        ret = self.biz.check_client_perm(file_id, client_id)
        body = dict(msg_type='rights_list',
                    params=dict(file_id=file_id,
                                rights=ret))
        self.send_message(body=json.dumps(body), routing_key=client_id)
    
    
    def _request_file(self, params, client_id):
        '''
        '''
        file_id = params['id']
        sha1 = params['sha1']
        peers, size = self.biz.get_file_peers(file_id, client_id, sha1,
                                        get_pub_key=False, get_file_size=True)
        body = dict(msg_type='slice_assign',
                    params=dict(file_id=file_id,
                                sha1=sha1))
        num_slices = int(ceil(float(size) / self.slice_size))
        slices_per = int(ceil(float(num_slices) / len(peers)))
        slices_sent = 0
        
        for p in peers:
            for n in xrange(slices_per):
                body['params']['slice_num'] = slices_sent
                self.send_message(body=json.dumps(body),
                                  routing_key=p['user_id'])
                slices_sent += 1
                if slices_sent >= num_slices: break
                n # Shut up the stupid eclipse warning














