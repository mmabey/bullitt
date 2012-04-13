#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin
'''

from daemon import Client

file_dict = None
daemon = Client()
    
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
    file_id = raw_input("Enter file id from listfiles: ")
    
    #TODO: implement lookups
    filename = None
    prev_sha1 = None
    file_uuid = None
    
    daemon.add_or_mod_file(filename, prev_sha1, file_uuid)
    
    
def delete_file():
    '''
    Tell index server to drop a file
    '''
    
    #TODO: implement lookup
    file_uuid = None
    sha1_hash = None
    
    daemon.delete_file(file_uuid, sha1_hash)
    
def get_peers():
    '''
    Shows all the other client vms in the system w/ access to a file
    '''
  
def client_lookup():
    print daemon.client_lookup()  
    
def grant_rights():
    '''
    grant a user rights on a file
    '''
    client_lookup()
    
    
    
def revoke_rights():
    '''
    revoke rights on a file
    '''
    
def query_rights():
    '''
    Query rights
    '''
    file_id = raw_input("Enter file id: ")
    
    #TODO: implement lookup
    file_uuid = None
    
    daemon.query_rights(file_uuid)
        
def list_files():
    '''
    Display list of files which this user
    has access to
    '''
    file_dict = daemon.list_files()
    print file_dict
    
def version_downloaded():
    '''
    notify the server of what version you have
    '''
    
def request_file():
    '''
    PULL a file into the local store from peers
    '''
    
    file_id = raw_input("Enter id from list_files: ")
    
    #TODO: lookup file uuid, sha1, and bytes
    file_uuid = None
    sha1_hash = None
    bytes = None
    daemon.request_file(file_uuid, sha1_hash, bytes)
    

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
            resp = raw_input(" > ")
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
            
    print

if __name__ == '__main__':
    main()