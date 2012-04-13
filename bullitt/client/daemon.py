#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin

'''

#TODO: generate public keys and client uuids and store info in json and IP

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

    def __init__(self, host, port, virtual_host, credentials, channel_max, frame_max, heartbeat):
        #parent constructor
        #TODO: Uncomment for deploy
        #RabbitObj.__init__(self, host, port, virtual_host, credentials, channel_max, frame_max, heartbeat)
        
        #read slice size from our config json
        gen_config_json = open("../common/gen_config.json")
        json_data = json.load(gen_config_json)
        self.CONST_SLICE_SIZE = json_data["slice_size"]

    def choochoo(self):
        '''
        Pretty much the best method ever
        '''
        subprocess.call("sl")
    
    def slice_file(self, filename, slice_number):
        '''
        Grabs the requested slice of the file @ filename
        Slice size is defined in ../common/gen_config.json
        But is currently using 180KB slices
        
        The 180KB slice size come from the argment
        http://www.unitethecows.com/p2p-general-discussion/30807-rodi-anonymous-p2p.html#post203703
        
        If successful returns the file_slice of the file as an object
        '''
        try:
            file_size = os.path.getsize(filename)
            start_byte = self.CONST_SLICE_SIZE * slice_number
            #end_byte = start_byte + self.CONST_SLICE_SIZE - 1 #not needed
    
            if start_byte < file_size and slice_number >= 0: #check that the file_slice number makes sense
                #subprocess.call("dd if={0} of={0}.file_slice{1} ibs={2} skip={1} count=1"\
                #                .format(filename, slice_number, self.CONST_SLICE_SIZE))
                #
                #the following code is the equivalent of the command above
                fhandle = open(filename, 'rb')
                fhandle.seek(start_byte) #move to correct byte
                file_slice = fhandle.read(self.CONST_SLICE_SIZE) #grab the file_slice
                #these two lines would complete the of= part of dd
                #ofile = open(filename + ".file_slice" + str(slice_number), 'wb') 
                #ofile.write(file_slice)
                
                #cleanup
                #ofile.close()
                fhandle.close()
                
                return file_slice
            else:
                print "Slice is outside of file!"
        except OSError:
            print "File not found or other OSError"
    
    def reassemble_slices(self, slice_list, outfilename):
        '''
        Decrypt slices and put them back together
        '''
        outhandle = open(outfilename, "wb")
        for file_slice in slice_list:
            outhandle.write(file_slice)
    
        outhandle.close()

    def decrypt_slice(self):
        '''
        Decrypt slice duhhh
        '''
        #Use session key to decrypt via Crypto
        #This may be changed to decrypt the entire JSON message instead
        #In which case this will be called from the receive method
        #instead of the reassembly 

    def create_session_key(self):
        '''
        Create a session key
        '''
        #TODO: grab a crypto random number from Crypto

    def send_slice(self):
        '''
        Send slice to a vm
        '''
        #TODO: this will probably send a message via rabbitmq
        
    def send_to_server(self, filename, prev_sha1=None, file_uuid=None):
        '''
        Sends a file_handle to the server
        Including a previous hash and uuid implies an update
        '''
        
        #read data in as binary
        file_handle = open(filename, "rb")
        filedata = file_handle.read()
        file_handle.close()
        
        #grab file size
        file_size = os.path.getsize(filename)
        
        #generate sha1
        sha1_hash = hashlib.sha1(filedata).hexdigest()
        
        if file_uuid == None:
            file_uuid = uuid.uuid4() #create a uuid for new file_handle
        
        #TODO: package and send to server
        
    class MessageTask(threading.Thread):
        def run(self):
            '''
            This method will handle the sending/receiving of messages
            '''
            pass
        
if __name__ == '__main__':
    '''
    Purely a testing method - should be removed before deploying
    '''
    client = Client(None, None, None, None, None, None, None)
    client.send_to_server("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf")
    
    #num_slices = int(math.ceil(os.path.getsize("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf") / float(client.CONST_SLICE_SIZE)))
    #slices = list()
    #for x in range(num_slices):
    #    slices.insert(x, client.slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", x))
    #client.reassemble_slices(slices, "C:/Users/Justin/Desktop/trying/copy.pdf")
    pass
