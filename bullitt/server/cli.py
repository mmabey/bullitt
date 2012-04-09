'''
Created on Apr 2, 2012

@author: Mike
'''

# Library imports

# Third-party libraries

# Local imports

# Constants


def print_main_menu():
    '''
    Prints main CLI menu for the server.
    '''
    pass


def main():
    title = "Bullitt: The Collaborative Data Sharing System"
    print "\n%s\n%s\n" % (title, '-' * len(title))
    
    while True:
        print_main_menu()
        res = raw_input(">> ")
        if res.lower()[0] == 'q':
            break
        elif res == '1':
            pass
        elif res == '2':
            pass
        elif res == '3':
            pass


if __name__ == '__main__':
    pass
