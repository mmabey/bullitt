#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin

'''

# Library imports
import threading
import subprocess
import hashlib
import uuid #use str(uuid.uuid4())
import os
import math
import json

# Third-party libraries
import Crypto

# Local imports
from bullitt.common.cuffrabbit import RabbitObj

# Constants/Globals

class Client(RabbitObj):

    '''
    =====
    pull from JSON file gen_config
    =====
    '''

    def __init__(self, host, port, virtual_host, credentials, channel_max, frame_max, heartbeat):
        #parent constructor
        #TODO Uncomment for deploy
        #RabbitObj.__init__(self, host, port, virtual_host, credentials, channel_max, frame_max, heartbeat)
        
        #read slice size from our config json
        gen_config_json = open("../common/gen_config.json")
        json_data = json.load(gen_config_json)
        self.CONST_SLICE_SIZE = json_data["slice_size"]

    def choochoo(self):
        #learned to use subprocess left it because sl is wonderful
        #probably should remove since we're no longer using subprocess
        subprocess.call("sl")
    
    def slice_file(self, file, slice_number):
        '''
        Calls dd to split a file into 180KB blocks
        The slice argument for this slice size comes from
        http://www.unitethecows.com/p2p-general-discussion/30807-rodi-anonymous-p2p.html#post203703
        
        If successful returns the slice of the file as an object
        '''
        try:
            file_size = os.path.getsize(file)
            start_byte = self.CONST_SLICE_SIZE * slice_number
            #end_byte = start_byte + self.CONST_SLICE_SIZE - 1 #not needed
    
            if start_byte < file_size and slice_number >= 0: #check that the slice number makes sense
                #subprocess.call("dd if={0} of={0}.slice{1} ibs={2} skip={1} count=1"\
                #                .format(file, slice_number, self.CONST_SLICE_SIZE))
                #
                #the following code is the equivalent of the command above
                fhandle = open(file, 'rb')
                fhandle.seek(start_byte) #move to correct byte
                slice = fhandle.read(self.CONST_SLICE_SIZE) #grab the slice
                #these two lines would complete the of= part of dd
                #ofile = open(file + ".slice" + str(slice_number), 'wb') 
                #ofile.write(slice)
                
                #cleanup
                fhandle.close()
                #ofile.close()
                
                return slice
            else:
                print "Slice is outside of file!!"
        except OSError:
            print "File not found or other OSError"
    
    def reassemble_slices(self, slice_list, outfile):
        '''
        Decrypt slices and put them back together
        '''
        outhandle = open(outfile, "wb")
        for slice in slice_list:
            outhandle.write(slice)
    
    def decrypt_slice(self):
        '''
        Decrypt slice duhhh
        '''

    def create_session_key(self):
        '''
        Create a session key
        '''
    

    def send_slice(self):
        '''
        Send slice to a vm
        '''
    
if __name__ == '__main__':
    '''
    Purely a testing method - should be removed before deploying
    '''
    client = Client(None, None, None, None, None, None, None)
    num_slices = int(math.ceil(os.path.getsize("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf") / float(client.CONST_SLICE_SIZE)))
    slices = list()
    for x in range(num_slices):
        slices.insert(x, client.slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", x))
    client.reassemble_slices(slices, "C:/Users/Justin/Desktop/trying/copy.pdf")
    pass
