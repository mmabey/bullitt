#!/usr/bin/env python
'''
Created on Apr 2, 2012

@author: Mike
'''

# Library imports

# Third-party libraries

# Local imports
import index

# Constants/Globals
DEBUG = False
SERVER = index.Server()


def view_clients():
    clients = SERVER.get_clients()
    if not len(clients):
        print
        print "No clients recorded in the system yet.".center(79)
        print "Try adding some first.".center(79)
        print
        return False
    
    print '\n ', "Client UUID".center(36, '.'), '', "IP".center(15, '.'),
    print '', "Key".center(7, '.')
    
    for c in clients:
        print '  %s  %s  %s' % (c['user_id'].center(36),
                                c['ipaddr'].center(15),
                                len(c['pub_key']) == 272 and "Yes".center(7) \
                                    or "No".center(7))
    return True


def add_client():
    err = ''
    _uid = ''
    _ipaddr = ''
    _key = ''
    while err != []:
        if err:
            print '\n\n'
            for e in err:
                print "Error:", e
            print '\n\n'
        
        err = []
        uid = raw_input("Enter the client's UUID %s> " % _uid).strip()
        ipaddr = raw_input("\nEnter the client's IP address %s> " % _ipaddr).strip()
        key = raw_input("\nEnter the path to the client's public key %s> " % _key).strip()
        
        if len(uid) != 36 or (uid[8], uid[13], uid[18], uid[23]) != \
                ('-', '-', '-', '-') or not uid.replace('-', '').isalnum():
            if len(uid) == 0 and len(_uid):
                uid = _uid
            else:
                err.append('UUID of incorrect length/format.')
                _uid = ''
        else: _uid = uid
        
        if 15 < len(ipaddr) < 7 or ipaddr.count('.') != 3 or \
                not ipaddr.replace('.', '').isdigit():
            if len(ipaddr) == 0 and len(_ipaddr):
                ipaddr = _ipaddr
            else:
                err.append('IP address of incorrect length/format.')
                _ipaddr = ''
        else: _ipaddr = ipaddr
        
        try:
            if len(key) == 0 and len(_key):
                key = _key
            with open(key) as fin:
                key = fin.read()
        except IOError:
            err.append('Could not open file: %s' % key)
            _key = ''
        else:
            _key = key
        
    print "\nAdding client with IP: %s" % ipaddr
    SERVER.add_client(uid, ipaddr, key)
    print "Done.\n"


def del_client():
    resp = raw_input("Would you like to see a list of clients first? (Y/n) > ")
    if len(resp) == 0 or resp[0].lower() != 'n':
        if not view_clients(): return
    
    print "\n\nEnter the UUID or IP address of the client to delete."
    resp = raw_input(' > ').strip()
    
    params = {}
    if len(resp) == 36 and (resp[8], resp[13], resp[18], resp[23]) == \
            ('-', '-', '-', '-') and resp.replace('-', '').isalnum():
        params['client_id'] = resp
    elif 15 > len(resp) > 7 and resp.count('.') == 3 \
            and resp.replace('.', '').isnum():
        params['ipaddr'] = resp
    
    res = SERVER.del_client(**params)
    if res:
        print "\nDeletion successful.\n"
    else:
        print "\nDeletion FAILED. Please check your input and try again.\n"
        if DEBUG: print '\n%r\n' % res


def print_main_menu():
    '''
    Prints main CLI menu for the server.
    '''
    print
    print "Options:"
    print "--------"
    print
    print "  1. Add a client"
    print "  2. View list of clients"
    print "  3. Delete a client"
    print "  4. Show server activity"
    print "  Q or Ctrl+C to exit"
    print


def main():
    title = "Bullitt: The Collaborative Data Sharing System"
    print "\n%s\n%s\n" % (title.center(79), ('-' * len(title)).center(79))
    
    while True:
        print_main_menu()
        try:
            resp = raw_input(" > ")
        except (EOFError, KeyboardInterrupt):
            print
            break
        print
        if len(resp) and resp.lower()[0] == 'q':
            break
        elif resp == '1':
            add_client()
        elif resp == '2':
            view_clients()
        elif resp == '3':
            del_client()
        elif resp == '4':
            prevV = index.VERBOSE
            prevD = index.cuffrabbit.DEBUG
            index.VERBOSE = index.cuffrabbit.DEBUG = True
            print "\nPress Ctrl+C to return to the main menu."
            while True:
                try:
                    raw_input()
                except KeyboardInterrupt:
                    print
                    break
            index.VERBOSE = prevV
            index.cuffrabbit.DEBUG = prevD
    print


if __name__ == '__main__':
    main()
