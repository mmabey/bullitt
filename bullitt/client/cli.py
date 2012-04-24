#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin
'''

import subprocess
import daemon as DAE

file_dict = None
daemon = DAE.Client()
    
def add_file():
    '''
    PUT a file's info into the index server
    '''
    filepath = raw_input("Enter filepath: ")
    daemon.add_or_mod_file(filepath)
    
def mod_file():
    '''
    Notify index server of change to file
    '''
    file_id = raw_input("Enter file id from listfiles > ").strip()
    
    #TODO: implement lookups
    files = daemon.list_files()
    filename = None
    prev_sha1 = None
    for f in files:
        if f['file_id'] == file_id:
            filename = f['file_name']
            prev_sha1 = f['sha1']
    if filename == None:
        print
        print "Invalid file UUID. Please try again.".center(80)
        print
        return
    
    daemon.add_or_mod_file(filename, prev_sha1, file_id)
    
    
def delete_file():
    '''
    Tell index server to drop a file
    '''
    
    #TODO: implement lookup
    file_uuid = raw_input("Enter the file's UUID to be deleted > ").strip()
    print
    sha1_hash = raw_input("Enter the SHA1 hash of the file > ").strip()
    print
    print
    
    daemon.delete_file(file_uuid, sha1_hash)


def get_peers():
    '''
    Shows all the other client vms in the system w/ access to a file
    '''
    file_uuid = raw_input("Enter the file UUID to get its peers > ").strip()
    peers = daemon.get_peers(file_uuid)
    if peers == None:
        print
        print "No peers listed. Either you have no rights on the".center(80)
        print "file, or it hasn't been shared with any clients.".center(80)
        print
        return
    
    print "Peers".center(38)
    for p in peers:
        print p
    
  
def client_lookup():
    clients = daemon.client_lookup()
    if clients == None:
        print
        print "There are no other clients in the system."
        print
        return False
    print "Clients in the System".center(38, '.')
    for c in clients:
        print c['user_id'], c['ipaddr']
    return True


def grant_rights():
    '''
    grant a user rights on a file
    '''
    if not client_lookup(): return
    try:
        file_uuid = raw_input("File UUID > ").strip()
        client_uuid = raw_input("Client being granted > ").strip()
        read = int(raw_input("Grant READ right? (0/1) > ").strip())
        write = int(raw_input("Grant WRITE right? (0/1) > ").strip())
        daemon.grant_rights(file_uuid, client_uuid, read, write)
    except:
        print
        print "Oops. Something bad happened. Please try again.".center(80)


def revoke_rights():
    '''
    revoke rights on a file
    '''
    file_uuid = raw_input("File UUID > ").strip()
    client_uuid = raw_input("Client being revoked > ").strip()
    read = int(raw_input("Revoke READ right? (0/1) > ").strip())
    write = int(raw_input("Revoke WRITE right? (0/1) > ").strip())
    daemon.revoke_rights(file_uuid, client_uuid, read, write)


def query_rights():
    '''
    Query rights
    '''
    file_id = raw_input("Enter file id: ")
    
    #TODO: implement lookup
    #file_uuid = None
    
    print "You have the following permissions:"
    print daemon.query_rights(file_id)


def list_files():
    '''
    Display list of files which this user
    has access to
    '''
    files = daemon.list_files()
    if files == None or len(files) <= 0:
        print
        print "You have no access to any files in the system.".center(80)
        print "Try uploading some first.".center(80)
        print
        return
    
    # Header: 
    print " %s  %s  %s  %s  %s" % ("File Name".center(15, '.'),
                                    "File ID".center(38, '.'),
                                    "RWO",
                                    "Bytes".center(7, '.'),
                                    "SHA1".center(8, '.'))
    
    for f in files:
        print " %s  %s  %d%d%d  %6d\n%s" % \
            (f['file_name'].center(15), f['file_id'].center(38), int(f['read']),
             int(f['write']), int(f['owner']), f['size'], f['sha1'].rjust(80))
    print 


def version_downloaded():
    '''
    notify the server of what version you have
    '''


def request_file():
    '''
    PULL a file into the local store from peers
    '''
    
    file_id = raw_input("Enter id from list_files: ")
    sha1_hash = raw_input("Enter sha1 hash from list_files: ")
    bytes = raw_input("Enter the number of bytes needed: ")
    
    daemon.request_file(file_id, sha1_hash, bytes)
    

def print_main_menu():
    menu = """    1. Add a file
    2. Update modified file
    3. Delete a file
    4. Get peers
    5. Grant rights
    6. Revoke rights
    7. Query rights
    8. List files
    9. Request file"""
    
    print menu

def main():
    title = "Bullitt Client: The Collaborative Data Sharing System"
    print "{0}\n{1}".format(title.center(80), ('=' * len(title)).center(80))
    
    while True:
        print_main_menu()
        try:
            resp = raw_input(" > ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        print
        
        if len(resp) and resp.lower()[0] == 'q':
            break
        elif resp == '1':
            add_file()
        elif resp == '2':
            mod_file()
        elif resp == '3':
            delete_file()
        elif resp == '4':
            get_peers()
        elif resp == '5':
            grant_rights()
        elif resp == '6':
            revoke_rights()
        elif resp == '7':
            query_rights()
        elif resp == '8':
            list_files()
        elif resp == '9':
            request_file()
        elif resp == '666':
            subprocess.call('sl')
        elif resp == '0':
            # Switch on/off Debugging
            DAE.VERBOSE = not DAE.VERBOSE
            DAE.DEBUG = not DAE.DEBUG
            
    print

if __name__ == '__main__':
    main()
