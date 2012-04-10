'''
Created on Apr 2, 2012

@author: Mike
'''

# Library imports

# Third-party libraries

# Local imports
from index import Server

# Constants/Globals
SERVER = Server()


def print_main_menu():
    '''
    Prints main CLI menu for the server.
    '''
    print
    print "Options:"
    print "--------"
    print
    print "  1. View list of clients"
    print "  2. Add a client"
    print "  3. Delete a client"
    print "  Q to exit"
    print


def view_clients():
    print "Clients: ..."


def add_client():
    err = 'boo!'
    while err != []:
        uid = raw_input("Enter the client's UUID > ").strip()
        ipaddr = raw_input("\nEnter the client's IP address > ").strip()
        key = raw_input("\nEnter the path to the client's public key > ").strip()
        
        if len(uid) != 36 or (uid[8], uid[13], uid[18], uid[23]) != \
                ('-', '-', '-', '-') or not uid.replace('-', '').isalnum():
            err.append('UUID of incorrect length/format.')
        if len(ipaddr) < 7 or ipaddr.count('.') != 3 or not \
                ipaddr.replace('.', '').isnum():
            err.append('IP address of incorrect length/format.')


def del_client():
    print "delete delete"


def main():
    title = "Bullitt: The Collaborative Data Sharing System"
    print "\n%s\n%s\n" % (title, '-' * len(title))
    
    while True:
        print_main_menu()
        try:
            res = raw_input(" > ")
        except (EOFError, KeyboardInterrupt):
            break
        print
        if len(res) and res.lower()[0] == 'q':
            break
        elif res == '1':
            view_clients()
        elif res == '2':
            add_client()
        elif res == '3':
            del_client()


if __name__ == '__main__':
    main()
