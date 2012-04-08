#!/usr/bin/env python
'''
Created on Apr 5, 2012

@author: mmabey
'''

# Library imports

# Third-party libraries

# Local imports
from bullitt.common import cuffrabbit
from db import ServerBiz

# Constants
DEBUG = False
INFO = True



class Server(cuffrabbit.RabbitObj):
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
        self.biz = ServerBiz()
    
    
    def main(self):
        '''
        '''
        #TODO: Start child threads
        
        if INFO: print "Enter Ctrl+C to exit"
        # Enter main loop
        while True:
            try:
                resp = raw_input()
            except KeyboardInterrupt:
                break
            else:
                if resp.lower()[0] == 'q':
                    break
        if INFO: print "Shutting down child threads..."
        
        #TODO: Use join() on all child threads
        
        if INFO: print "Exiting"
