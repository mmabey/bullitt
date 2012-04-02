#!/usr/bin/env python
'''
Created on Apr 2, 2012

@author: mmabey
'''

# Library imports
import uuid

# Third-party libraries
import sqlalchemy

# Local imports

# Constants
__all__ = ['ServerBiz']

class ServerBiz(object):
    '''
    Handles all business logic of the server.
    
    Classes wishing to interact with the database should call this object in 
    lieu of the BullittSQL class, since this is where enforcement of policy
    and checking of input/output is performed.
    '''


    def __init__(self, params):
        '''
        
        '''
        
    
    def check_client_perm(self, file_id, client_id):
        '''
        Given a file and client ID, returns the permissions, None if NULL.
        '''
        pass
    
    
    def get_file_peers(self, file_id):
        '''
        For a given file, returns the list of clients with a copy of it.
        '''
        pass
    
    
    def update_file(self, file_id, client_id, op, info):
        '''
        Generic update method for adding, deleting, or modifying a file.
        
        file_id   -- UUID of file on which the operation is being performed
        client_id -- UUID of client performing the operation on the file
        op        -- 
        info      -- Additional info needed for operation
        '''
        pass
    
    
    def _add_file(self):
        '''
        '''
        pass
    
    
    def _del_file(self):
        '''
        '''
        pass
    
    
    def _mod_file(self):
        '''
        '''
        pass
    
    
    def update_client(self):
        '''
        '''
        pass
    
    
    def _add_client(self):
        '''
        '''
        pass
    
    
    def _del_client(self):
        '''
        '''
        pass
    
    
    def _mod_client_perm(self):
        '''
        '''
        pass
    
    
    #def



class BullittSQL(object):
    '''
    Data-level interaction with the database.
    '''


    def __init__(self, params):
        '''
        '''
        self.create_tables()
    
    
    def create_tables(self):
        '''
        Creates (destructive) the database and tables for the system.
        
        This method should only be called when the system is first being 
        initialized and when it is necessary to clear out the database
        completely.  Make sure you really want to do this before calling it.
        '''
        pass















