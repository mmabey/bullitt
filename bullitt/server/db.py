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
                       Integer, Boolean, PickleType, and_, not_, or_
from sqlalchemy.sql import select

# Local imports

# Constants
__all__ = ['ServerBiz']
LOGGING = False
DEBUG = False



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
    
    
    def add_client(self, client_id, ipaddr, pub_key, alias=None):
        '''
        
        Add a client to the system with the given ID, IP address, and pub key.
        '''
        self.db.insert_user(client_id, pub_key, ipaddr, alias)
    
    
    def get_clients(self):
        '''
        Return clients' info in a tuple of dicts where column names are keys.
        '''
        return self.db.select_users_all()
    
    
    def del_client(self, client_id):
        '''
        Delete the client if it exists.  Return false otherwise.
        '''
        res = self.client_exists(client_id)
        if res:
            print "Client exists: %s" % client_id
            return self.db.delete_user(client_id)
        print "Client does not exist: %s" % client_id
        print res
        return False
    
    
    def client_exists(self, client_id):
        '''
        Return if the given client_id is in the system.
        '''
        return self.db.select_user(client_id, field='user_id')
    
    
    def file_exists(self, file_id):
        '''
        Return if the file is already in the system.
        '''
        try:
            return bool(self.db.select_file(file_id)['file_id'])
        except TypeError:
            return False
    
    
    def perm_exists(self, file_id, client_id):
        '''
        Return if the client has a permission entry on the given file.
        '''
        return bool(self.check_client_perm(file_id, client_id))
        
    
    def check_client_perm(self, file_id, client_id):
        '''
        Return the permissions a client has on a file as a dict, None if NULL.
        '''
        return self.db.select_perm(file_id, client_id)
    
    
    def can_update(self, file_id, client_id):
        '''
        Shortcut to check for write or own permissions
        '''
        ret = self.check_client_perm(file_id, client_id)
        return ret['write'] or ret['owner']
    
    
    def get_file_owner(self, file_id, client_id=None):
        '''
        For a given file, return the UUID of the owner client.
        
        If a client_id is specified, return if the file's owner is that client.
        '''
        ret = self.db.select_file(file_id)
        if not ret: return None
        
        owner_id = ret['owner_id']
        if client_id == None:
            return owner_id
        return owner_id == client_id
    
    
    def get_file_peers(self, file_id, client_id, sha1=None, get_pub_key=True,
                       get_file_size=False):
        '''
        For a given file, return the list of clients with a copy of it.
        
        Does not return the result if the client does not have read or write
        permissions on the file.
        '''
        # Check the client's permissions by checking if it is in the list
        # of users with a permission on it.
        peers = self.db.select_file_peers(file_id, sha1, get_pub_key)
        try:
            peers.remove(client_id)
        except ValueError:
            return
        if get_file_size:
            return peers, self.db.select_file(file_id)['size']
        return peers
    
    
    def get_file_version(self, file_id):
        '''
        Return the current SHA1 hash of the file.
        '''
        try:
            return self.db.select_file(file_id)['sha1']
        except TypeError:
            return
    
    
    def get_client_files(self, client_id):
        '''
        Return the list of files to which a client has access.
        '''
        return self.db.select_user_files(client_id)
    
    
    # CLIENT OPERATION METHODS BEGIN HERE
    # -----------------------------------
    #
    # Many of the following methods are direct implementations of the
    # corresponding client operation.  In other words, there are no methods on
    # the index server that intermediates for them.  To help distinguish 
    # between the methods that do and do not have an intermediating method on 
    # the index server, each is marked by a comment after the method signature.
    
    def add_file(self, params, client_id): # Biz only
        '''
        Add a file to the system.  Set the client as the owner.
        '''
        file_id = params['id']
        file_name = params['name']
        size = params['bytes']
        sha1 = params['sha1']
        self.db.insert_file(file_id, client_id, file_name, sha1, size)
    
    
    def mod_file(self, params, client_id): # Biz only
        '''
        Update the file entry with current information.
        
        The operation is not executed if the client does not have write or own
        permissions on the file or if the previous hash given does not match
        the current hash in the database.
        '''
        file_id = params['id']
        sha1 = params['sha1']
        size = params['bytes']
        # Check that the SHA1 matches the current version.
        if self.can_update(file_id, client_id) \
                and params['prev_sha1'] == self.get_file_version(file_id):
            return self.db.update_file(file_id=file_id, sha1=sha1, size=size)
        return False
    
    
    def del_file(self, params, client_id): # INDEX
        '''
        Delete the file entry if the client is the owner.
        '''
        file_id = params['id']
        sha1 = params['sha1']
        if self.get_file_owner(file_id, client_id) and \
                self.get_file_version(file_id) == sha1:
            delfile, delperm = self.db.delete_file(file_id)
            return bool(delfile and delperm)
        return False
    
    
    def grant(self, params, client_id): # Biz only
        '''
        Grant a client more rights on a file.  Only read/write supported.
        
        Only the owner of the file can successfully grant other clients rights
        on that file.  If the grantee does not already have permissions on the
        file, a new permissions entry is created.
        '''
        file_id = params['id']
        grantee = params['client']
        read = bool(params['read'])
        if read == False: read = None
        
        write = bool(params['write'])
        if write == False: write = None
        
        if self.get_file_owner(file_id, client_id) and \
                self.client_exists(grantee):
            params2 = dict(file_id=file_id,
                           client_id=grantee,
                           read=read,
                           write=write)
            if self.perm_exists(file_id, grantee):
                func = self.db.update_perm
            else:
                params2['sha1'] = self.get_file_version(file_id)
                func = self.db.insert_perm
            try:
                func(**params2)
            except ValueError:
                # Granter specified (None, None) permissions. That's okay.
                pass
    
    
    def revoke(self, params, client_id): # Biz only
        '''
        Remove rights from a client on a file.  Only read/write supported.
        '''
        file_id = params['id']
        grantee = params['client']
        read = params['read']
        if read == True: read = None
        
        write = params['write']
        if write == True: write = None
        
        if self.get_file_owner(file_id, client_id) and \
                self.client_exists(client_id) and self.perm_exists(file_id,
                                                                   grantee):
            try:
                self.db.update_perm(file_id=file_id, client_id=grantee,
                                    read=read, write=write)
            except ValueError:
                # Granter specified (None, None) permissions. That's okay.
                pass
    
    
    def version_downloaded(self, params, client_id): # Biz only
        '''
        Store the SHA1 of the file's version that the client has.
        '''
        file_id = params['id']
        sha1 = params['sha1']
        self.db.update_user_version(file_id, client_id, sha1)
    
    
    def client_lookup(self, client_id=None, ipaddr=None, pub_key=None):
        '''
        Return all info on a client based on its ID, IP address, or public key.
        
        Info is returned as a dictionary where the column names are the keys.
        '''
        return self.db.select_user(client_id, ipaddr, pub_key)



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
                                Column('pub_key', PickleType, nullable=False),
                                Column('ipaddr', String(15), nullable=False),
                                Column('alias', String(5), nullable=True))
        self.file_table = Table('file_list', self.metadata,
                                Column('file_id', String(36), primary_key=True,
                                       nullable=False),
                                Column('file_name', String(128),
                                       nullable=False),
                                Column('owner_id', String(36), nullable=False),
                                Column('sha1', String(40), nullable=False),
                                Column('prev_sha1', String(40), nullable=True),
                                Column('size', Integer, nullable=False))
        self.perm_table = Table('permissions', self.metadata,
                                Column('file_id', String(36), primary_key=True,
                                       nullable=False),
                                Column('user_id', String(36), primary_key=True,
                                       nullable=False),
                                Column('sha1', String(40), nullable=False),
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
    
    
    def insert_user(self, client_id, pub_key, ipaddr, alias=None):
        '''
        Insert the fields for a new client.
        '''
        uvals = dict(user_id=client_id,
                     pub_key=pub_key,
                     ipaddr=ipaddr,
                     alias=alias)
        return self.user_table.insert().execute(**uvals)
    
    
    def select_user(self, client_id=None, ipaddr=None, pub_key=None,
                    field=None):
        '''
        Retrieve the client entry corresponding to the given information.
        
        Either client_id, ipaddr, or pub_key must be specified to successfully
        select the user, or else None will be returned.  To retrieve the entire
        entry row, field should remain None.  To retrieve a specific column, 
        give it as field.
        
        Returns a dictionary where the column names are the keys.
        '''
        where = None
        wherechanged = False
        if client_id is not None:
            where = self.user_table.c.user_id == client_id
            wherechanged = True
        elif ipaddr is not None:
            where = self.user_table.c.ipaddr == ipaddr
            wherechanged = True
        elif pub_key is not None:
            where = self.user_table.c.pub_key == pub_key
            wherechanged = True
        
        if not wherechanged:
            if DEBUG: print "Didn't make a WHERE properly..."
            return
        
        if field == None:
            field = [self.user_table]
        else:
            if DEBUG: print self.user_table.c.__dict__
            field = [self.user_table.c.__dict__['_data'][field]]
        
        result = self.conn.execute(select(field, where))
        row = result.fetchone()
        result.close()
        if row == None: return None
        keys = tuple([col.name for col in tuple(self.user_table.columns)])
        return dict(zip(keys, row))
    
    
    def select_users_all(self):
        '''
        Retrieve data for all users in the system.
        
        This method is primarily for use with testing systems and should be 
        used with discretion at any other time.
        '''
        result = self.conn.execute(select([self.user_table]))
        users = []
        keys = tuple([col.name for col in tuple(self.user_table.columns)])
        for row in result:
            users.append(dict(zip(keys, row)))
        return users
    
    
    def select_user_files(self, client_id):
        '''
        Return the files to which the client has access to.
        
        The files are returned as a tuple of dictionaries, where each one is a 
        returned result where the column names are the keys.
        '''
        s = select([self.file_table, self.perm_table.c.read,
                    self.perm_table.c.write, self.perm_table.c.owner],
                   and_(self.perm_table.c.user_id == client_id,
                        self.perm_table.c.file_id == self.file_table.c.file_id,
                        or_(self.perm_table.c.read == True,
                            self.perm_table.c.write == True,
                            self.perm_table.c.owner == True)))
        result = self.conn.execute(s)
        files = []
        keys = [col.name for col in tuple(self.file_table.columns)]
        keys += ['read', 'write', 'owner']
        for row in result:
            files.append(dict(zip(keys, row)))
        return files
    
    
    def update_user_version(self, file_id, client_id, sha1):
        '''
        '''
        ret1 = self.perm_table.update(and_(
                                        self.perm_table.c.file_id == file_id,
                                        self.perm_table.c.user_id == client_id))
        return ret1.execute(sha1=sha1)
    
    
    def delete_user(self, client_id):
        '''
        Delete the user entry matching the given ID. Delete perm entries too.
        '''
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
                     prev_sha1=None,
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
            field = [getattr(self.file_table.c, field)]
        s = select(field, self.file_table.c.file_id == file_id)
        result = self.conn.execute(s)
        row = result.fetchone()
        result.close()
        if row == None: return
        keys = tuple([col.name for col in tuple(self.file_table.columns)])
        return dict(zip(keys, row))
    
    
    def select_file_peers(self, file_id, sha1=None, get_pub_key=True):
        '''
        Return a list of client IDs that have permissions on the given file.
        
        If a client has been reduced to no permissions on the file (i.e. both
        read and write fields are False), it will not be included in the list.
        '''
        if sha1 == None:
            sha1 = self.perm_table.c.sha1 == self.file_table.c.sha1
        elif sha1 == 'all':
            sha1 = True
        else:
            sha1 = self.perm_table.c.sha1 == sha1
        
        fields = [self.perm_table.c.user_id]
        keys = ['user_id']
        if get_pub_key:
            fields.append(self.user_table.c.pub_key)
            keys.append('pub_key')
        
        s = select(fields,
                   and_(self.perm_table.c.file_id == file_id,
                        self.perm_table.c.user_id == self.user_table.c.user_id,
                        or_(self.perm_table.c.write == True,
                            self.perm_table.c.read == True),
                        sha1,
                        )
                   )
        result = self.conn.execute(s)
        peers = []
        for row in result:
            peers.append(dict(zip(keys, row)))
        result.close()
        return peers
    
    
    def update_file(self, file_id, sha1, size):
        '''
        For an existing file record, update the hash and size.
        
        Handles saving the previous SHA1 hash and setting as the prev_sha1
        for the updated entry.
        '''
        try:
            # If we were passed a hash object, get the hex digest string
            sha1 = sha1.hexdigest()
        except AttributeError:
            pass
        prev_sha1 = self.select_file(file_id)['sha1']
        fvals = dict(sha1=sha1, prev_sha1=prev_sha1, size=size)
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
    
    
    def insert_perm(self, file_id, client_id, read=False, write=False,
                    sha1=None):
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
                     sha1=sha1
                     )
        return self.perm_table.insert().execute(**pvals)
    
    
    def select_perm(self, file_id, client_id):
        '''
        Return the permissions of the given client on the given file.
        
        Permissions are returned as a dictionary with the column names as keys.
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
        if row == None: return
        return dict(zip(('read', 'write', 'owner'), row))
    
    
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


    













