#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin
'''

import subprocess

def print_main_menu():
    '''
    Prints main CLI menu for the client.
    '''
    print "CHOO CHOO"
    subprocess.call("sl")
    
def put():
    '''
    PUT a file's info into the index server
    '''
    
def list_files():
    '''
    Display list of files which this user
    has access to
    '''
    
def pull():
    '''
    PULL a file into the local store from peers
    '''

def main():
    title = "Bullitt Client: The Collaborative Data Sharing System"
    print "{0}\n{1}".format(title.center(80), ('=' * len(title)).center(80))

if __name__ == '__main__':
    main()