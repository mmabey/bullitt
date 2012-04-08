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
import os
import uuid

# Third-party libraries
from sqlalchemy import create_engine, MetaData, Table, Column, String, \
                       Integer, Boolean, and_, not_
from sqlalchemy.sql import select

# Local imports

# Constants
__all__ = ['ServerBiz']
LOGGING = False



def get_hash(obj, hashalg='sha1'):
    '''
    Return the hash of the passed object.
    
    :param obj:
    Can be either a string or file object.  If obj is a str, tries to open it
    as a file first and hash the contents of the file.  If that fails, the 
    string itself is hashed.  If obj is a file, the file is read and hashed.
    
    :param hashalg:
    Specifies the hashing algorithm that should be used on obj.  Must be an
    algorithm provided by hashlib.  Defaults to 'sha1'.
    '''
    if type(obj) == str or type(obj) == unicode:
        try:
            with open(obj) as fin:
                return hashlib.__dict__[hashalg](fin.read())
        except IOError:
            return hashlib.__dict__[hashalg](obj)
    elif type(obj) == file:
        return hashlib.__dict__[hashalg](file.read())


def get_id():
    '''
    Return a new UUID.
    '''
    return uuid.uuid4()



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
        self.db = BullittSQL()
        
    
    def check_client_perm(self, file_id, client_id):
        '''
        Return the permissions a client has on a file as a dict, None if NULL.
        '''
        res = self.db.select_perm(file_id, client_id)
        if res == None: return
        return dict(zip(('read', 'write', 'owner'), res))
    
    
    def get_file_owner(self, file_id):
        '''
        For a given file, return the UUID of the owner client.
        '''
        return self.db.select_file(file_id, 'owner_id')
    
    
    def get_file_peers(self, file_id, client_id):
        '''
        For a given file, return the list of clients with a copy of it.
        
        Does not return the result if the client does not have read or write
        permissions on the file.
        '''
        # Check the client's permissions by checking if it is in the list
        # of users with a permission on it.
        peers = self.db.select_file_peers(file_id)
        try:
            peers.remove(client_id)
        except ValueError:
            return
        return peers
    
    
    def update_file(self, file_id, client_id, op, info):
        '''
        Generic update method for adding, deleting, or modifying a file.
        
        :param file_id:
        UUID of file on which the operation is being performed
        
        :param client_id:
        UUID of client performing the operation on the file
        
        :param op:
        
         
        :param info:
        Additional info needed for operation
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
        Constructor.
        
        Initializes the configuration of the object from server_config.json.
        Creates an engine object and makes a connection to the database.  Forms
        a metadata object and calls create_tables().
        '''
        # Get the path to where this file is stored - Prevents an error when 
        # opening the JSON config file.
        p = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(p, 'server_config.json')) as fin:
            config = json.load(fin)
        
        self.metadata = MetaData()
        self.engine = create_engine('%s%s://%s:%s@%s/%s' % \
                                    (config['db_type'],
                                     config['db_conn'] != '' and \
                                     '+' + config['db_conn'] or '',
                                     config['sql_user'],
                                     config['sql_pass'],
                                     config['db_url'],
                                     config['db_name']),
                                    echo=LOGGING)
        self.conn = self.engine.connect()
        self.metadata.bind = self.engine
        
        # Define the tables for the database
        self.user_table = Table('user_list', self.metadata,
                                Column('user_id', String(36), primary_key=True,
                                       nullable=False),
                                #TODO: Determine data type for public keys
                                #TODO: What other fields are necessary?
#                                Column('pub_key', ?, nullable=False)
                                )
        self.file_table = Table('file_list', self.metadata,
                                Column('file_id', String(36), primary_key=True,
                                       nullable=False),
                                Column('file_name', String(128),
                                       nullable=False),
                                Column('owner_id', String(36), nullable=False),
                                Column('sha1', String(40), nullable=False),
                                Column('size', Integer, nullable=False))
        self.perm_table = Table('permissions', self.metadata,
                                Column('file_id', String(36), primary_key=True),
                                Column('user_id', String(36), primary_key=True),
#                                Column('user_key', String(), nullable=False),
                                Column('read', Boolean, nullable=False),
                                Column('write', Boolean, nullable=False),
                                Column('owner', Boolean, nullable=False))
        
        self.table_list = dict(user=self.user_table,
                               file=self.file_table,
                               perm=self.perm_table)
        self.create_tables()
    
    
    def create_tables(self, drop=False):
        '''
        Creates the database and tables for the system.
        
        :param drop:
        Defaults to False, drop the tables before adding them, effectively 
        resetting the entire database.
        '''
        if drop:
            # Drop all the tables associated with the metadata object
            self.file_table.drop(self.engine)
            self.perm_table.drop(self.engine)
        # Create the tables
        self.metadata.create_all()
    
    
    def insert_user(self, client_id, pub_key):
        '''
        Insert the fields for a new client.
        '''
        uvals = dict(user_id=client_id,
                     #TODO: Add other fields of table below
                     #pub_key
                     )
        ures = self.user_table.insert().execute(**uvals)
        return ures
    
    
    def select_user(self, client_id, field=None):
        '''
        Retrieve the client entry corresponding to the given client_id.
        '''
        if field == None:
            field = [self.user_table]
        else:
            field = [self.user_table.c.__dict__[field]]
        result = self.conn.execute(select(field))
        row = result.fetchone()
        result.close()
        keys = tuple([col.name for col in tuple(self.user_table.columns)])
        return dict(zip(keys, row))
    
    
    def delete_user(self, client_id):
        ret1 = self.user_table.delete(self.user_table.c.user_id == client_id)
        ret1 = ret1.execute()
        
        ret2 = self.perm_table.delete(self.perm_table.c.user_id == client_id)
        ret2 = ret2.execute()
        return ret1, ret2
    
    
    def insert_file(self, file_id, client_id, file_name, sha1, size):
        '''
        Insert the data for a new file.
        
        Returns a 2-tuple with the results from the insert statements to the
        file_list and permissions table, respectively.
        '''
        try:
            # If we were passed a hash object, get the hex digest string
            sha1 = sha1.hexdigest()
        except AttributeError:
            pass
        fvals = dict(file_id=file_id,
                     file_name=file_name,
                     owner_id=client_id,
                     sha1=sha1,
                     size=size,
                     )
        pvals = dict(file_id=file_id,
                     user_id=client_id,
                     read=True,
                     write=True,
                     owner=True,
                     )
        fres = self.file_table.insert().execute(**fvals)
        pres = self.perm_table.insert().execute(**pvals)
        return fres, pres
    
    
    def select_file(self, file_id, field=None):
        '''
        Retrieve the file entry corresponding to the given file_id.
        
        :param field:
        To retrieve a specific field of a file entry, specify it here.  
        Defaults to None, which gets all fields.
        
        Entry fields are returned as a dictionary with the names of the fields
        as the keys.
        '''
        if field == None:
            field = [self.file_table]
        else:
            field = [self.file_table.c.__dict__[field]]
        result = self.conn.execute(select(field))
        row = result.fetchone()
        result.close()
        keys = tuple([col.name for col in tuple(self.file_table.columns)])
        return dict(zip(keys, row))
    
    
    def select_file_peers(self, file_id):
        '''
        Return a list of client IDs that have permissions on the given file.
        
        If a client has been reduced to no permissions on the file (i.e. both
        read and write fields are False), it will not be included in the list.
        '''
        s = select([self.perm_table.c.user_id],
                   and_(self.perm_table.c.file_id == file_id,
                        not_(and_(self.perm_table.c.write == False,
                                  self.perm_table.c.read == False)
                             )
                        )
                   )
        result = self.conn.execute(s)
        peers = []
        for row in result:
            peers.append(row)
        result.close()
        return peers
    
    
    def update_file(self, file_id, sha1, size):
        '''
        For an existing file record, update the hash and size.
        '''
        try:
            # If we were passed a hash object, get the hex digest string
            sha1 = sha1.hexdigest()
        except AttributeError:
            pass
        fvals = dict(sha1=sha1, size=size)
        ret1 = self.file_table.update(self.file_table.c.file_id == str(file_id))
        return ret1.execute(**fvals)
    
    
    def delete_file(self, file_id):
        '''
        Delete the file entry matching the given ID. Delete perm entries too.
        '''
        ret1 = self.file_table.delete(self.file_table.c.file_id == file_id)
        ret1 = ret1.execute()
        
        ret2 = self.perm_table.delete(self.perm_table.c.file_id == file_id)
        ret2 = ret2.execute()
        return ret1, ret2
    
    
    def insert_perm(self, file_id, client_id, read=False, write=False):
        '''
        Insert a new set of permissions for a client on a file. 
        '''
        if (read, write) == (False, False):
            raise ValueError('Parameter read or write must be True or 1.')
        
        pvals = dict(file_id=file_id,
                     user_id=client_id,
                     read=bool(read),
                     write=bool(write),
                     owner=False,
                     )
        return self.perm_table.insert().execute(**pvals)
    
    
    def select_perm(self, file_id, client_id):
        '''
        Return the permissions of the given client on the given file.
        
        Permissions are returned as a tuple of 3 boolean values:
        (read, write, owner)
        '''
        s = select([self.perm_table.c.read,
                    self.perm_table.c.write,
                    self.perm_table.c.owner],
                   and_(self.perm_table.c.file_id == file_id,
                        self.perm_table.c.user_id == client_id)
                   )
        result = self.conn.execute(s)
        row = result.fetchone()
        result.close()
        return row
    
    
    def update_perm(self, file_id, client_id, read=None, write=None):
        '''
        For an existing record, change the permissions for a client on a file.
        
        If both read and write are False, delete_perm() is called instead.
        '''
        if (read, write) == (None, None):
            raise ValueError('Parameter read or write must be specified.')
        elif (read, write) == (False, False):
            return self.delete_perm(file_id, client_id)
        
        pvals = dict()
        if read is not None:
            pvals['read'] = bool(read)
        if write is not None:
            pvals['write'] = bool(write)
        
        ret1 = self.perm_table.update(and_(
                                        self.perm_table.c.file_id == file_id,
                                        self.perm_table.c.user_id == client_id))
        return ret1.execute(**pvals)
    
    
    def delete_perm(self, file_id, client_id):
        '''
        Delete the permission entry matching the given file and user IDs.
        '''
        ret1 = self.perm_table.delete(and_(
                                       self.perm_table.c.file_id == file_id,
                                       self.perm_table.c.user_id == client_id))
        return ret1.execute()


    













