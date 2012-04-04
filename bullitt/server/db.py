#!/usr/bin/env python
'''
Created on Apr 2, 2012

@author: mmabey

For documentation on SQLAlchemy, visit: 
    http://docs.sqlalchemy.org/en/latest/core/tutorial.html
'''

# Library imports
import hashlib
import json
import uuid

# Third-party libraries
from sqlalchemy import create_engine, MetaData, Table, Column, String, \
                       Integer, Boolean

# Local imports

# Constants
__all__ = ['ServerBiz']
LOGGING = True


class ServerBiz(object):
    '''
    Handles all business logic of the server.
    
    Classes wishing to interact with the database should call this object in 
    lieu of the BullittSQL class, since this is where enforcement of policy
    and checking of input/output is performed.
    '''


    def __init__(self):
        '''
        
        '''
        
    
    def check_client_perm(self, file_id, client_id):
        '''
        Given a file and client ID, returns the permissions, None if NULL.
        '''
        # 
        pass
    
    
    def get_file_owner(self, file_id):
        '''
        For a given file, return the UUID of the owner client.
        '''
        pass
    
    
    def get_file_peers(self, file_id):
        '''
        For a given file, return the list of clients with a copy of it.
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
    
    
    def publish_update(self, file_id, checksum):
        '''
        Announce the given file has been updated to the new checksum.
        '''
        pass
    
    
    def _add_file(self, client_id,):
        '''
        Specialized version of modify operation.
        
        Inserts a new entry in the database for the file specified
        '''
        pass
    
    
    def _del_file(self):
        '''
        '''
        pass
    
    
    def _mod_file(self):
        '''
        Update the file index with the newest version of the file.
        
        
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


    def __init__(self):
        '''
        '''
        with open('server_config.json') as fin:
            config = json.load(fin)
        
        self.metadata = MetaData()
        self.metadata.bind = create_engine('%s%s://%s:%s@%s/%s' % \
                                           (config['db_type'],
                                            config['db_conn'] != '' and \
                                            '+' + config['db_conn'] or '',
                                            config['sql_user'],
                                            config['sql_pass'],
                                            config['db_url'],
                                            config['db_name']),
                                           echo=LOGGING)
        self.file_table = None
        self.perm_table = None
        self.create_tables()
    
    
    def create_tables(self, drop=False):
        '''
        Creates the database and tables for the system.
        
        :param drop:
        Defaults to False, drop the tables before adding them, effectively 
        resetting the entire database.
        '''
        
        # Define the tables for the database
        self.file_table = Table('file_list', self.metadata,
                                Column('file_id', String(36), primary_key=True,
                                       nullable=False),
                                Column('file_name', String(128),
                                       nullable=False),
                                Column('owner_id', String(36), nullable=False),
                                Column('sha1', String(40), nullable=False))
        self.perm_table = Table('permissions', self.metadata,
                                Column('pk', Integer, primary_key=True),
                                Column('file_id', String(36), nullable=False),
                                Column('user_id', String(36), nullable=False),
                                Column('read', Boolean, nullable=False),
                                Column('write', Boolean, nullable=False),
                                Column('owner', Boolean, nullable=False))
        if drop:
            # Drop all the tables associated with the metadata object
            self.metadata.drop_all()
        # Create the tables
        self.metadata.create_all()















