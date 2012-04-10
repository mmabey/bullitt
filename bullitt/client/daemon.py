#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin

INHERIT FROM RABBITOBJ
'''

import subprocess
import hashlib
import uuid #use str(uuid.uuid4())
import os
import math
import Crypto

'''
=====
pull from JSON file gen_config
=====
'''
CONST_SLICE_SIZE = 184320 #180KB slice size

def choochoo():
    #learned to use subprocess left it because sl is wonderful
    subprocess.call("sl")
    
def slice_file(file, slice_number):
    '''
    Calls dd to split a file into 180KB blocks
    The slice argument for this slice size comes from
    http://www.unitethecows.com/p2p-general-discussion/30807-rodi-anonymous-p2p.html#post203703
    '''
    
    try:
        file_size = os.path.getsize(file)
        start_byte = CONST_SLICE_SIZE * slice_number
        end_byte = start_byte + CONST_SLICE_SIZE - 1
    
        if start_byte < file_size:
            #print "dd if={0} of={0}.slice{1} ibs={2} skip={1} count=1".format(file, slice_number, CONST_SLICE_SIZE)
            subprocess.call("dd if={0} of={0}.slice{1} ibs={2} skip={1} count=1".format(file, slice_number, CONST_SLICE_SIZE))
        else:
            print "Slice is outside of file!!"
    except OSError:
        print "File not found or other OSError"
    
def reassemble_slices():
    '''
    Decrypt slices and put them back together
    '''
    
def decrypt_slice():
    '''
    Decrypt slice duhhh
    '''

def create_session_key():
    '''
    Create a session key
    '''
    

def send_slice():
    '''
    Send slice to a vm
    '''
    
if __name__ == '__main__':
    #num_slices = int(math.ceil(os.path.getsize("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf") / float(CONST_SLICE_SIZE)))
    #for x in range(num_slices):
    slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", 0)
    pass
