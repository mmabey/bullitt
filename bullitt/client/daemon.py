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
import base64

# Third-party libraries
import Crypto

# Local imports
from bullitt.common.cuffrabbit import RabbitObj

# Constants/Globals

class Client(RabbitObj):

    def __init__(self):
        
        #TODO: fix path to gen_config
        
        #read slice size from our config json
        gen_config_json = open("../common/gen_config.json")
        json_data = json.load(gen_config_json)
        self.CONST_SLICE_SIZE = json_data["slice_size"]
        self.rabbit_server = json_data["rabbit_server"]

        #parent constructor
        #TODO: Uncomment for deploy
        RabbitObj.__init__(self, self.rabbit_server)
        
        #TODO: implement reading the client.json

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

    def encrypt_data(self, data, pub_key):
        '''
        Encrypt some data using a given pubkey
        '''
        #TODO: write me
        
    def decrypt_slice(self):
        '''
        Decrypt slice with private key
        '''
        #Use session key to decrypt via Crypto
        #This may be changed to decrypt the entire JSON message instead
        #In which case this will be called from the receive method
        #instead of the reassembly 

        #TODO: write it

    def create_session_key(self):
        '''
        Create a session key
        '''
        #TODO: grab a crypto random number from Crypto

    def send_slice(self, slice, file_uuid, slice_num):
        '''
        Send slice to a vm
        
        Slices are encoded as base64 for transfer (JSON doesnt handle binary)
        '''
        #TODO: this will probably send a message via rabbitmq
        
        #generate sha1
        sha1_hash = hashlib.sha1(slice).hexdigest()
        
        #ensure uuid is a string
        file_uuid = str(file_uuid)
        
        message = {
                   "msg_type": "send_slice",
                   "params"  : {
                                "id"   : file_uuid,
                                "sha1" : sha1_hash,
                                "num"  : slice_num,
                                "slice": base64.b64encode(slice)
                                }
                   }
        
        json_object = json.dumps(message)
        #print json_object #debug
        
        #debug - prove that it decodes properly
        #print slice == base64.b64decode(base64.b64encode(slice))
        
        #TODO: encrypt the json object
        #TODO: fire off to RMQ server
        
    def add_or_mod_file(self, filename, prev_sha1=None, file_uuid=None):
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
        
        #if this is a new file
        if file_uuid == None:
            file_uuid = uuid.uuid4() #create a uuid for new file_handle
        
        file_uuid = str(file_uuid) #covert to string
        
        message = {
                   "params":
                            {"id":file_uuid, 
                             "name":filename,
                             "bytes":file_size,
                             "sha1":sha1_hash,
                            }
                   }
        
        if prev_sha1 == None:
            #adding file to system
            message["msg_type"]  = "add_file" 
        else:
            #modifying file in system
            message["msg_type"] = "mod_file" 
            message["params"]["prev_sha1"] = prev_sha1
            
        json_object = json.dumps(message)
        print json_object #debug
        
        #TODO: encrypt object
        #TODO: fire off to sender thread
        
    def delete_file(self, file_uuid, sha1_hash):
        '''
        Send message to delete file from server
        '''
        message = {
                   "msg_type": "del_file",
                   "params"  : {
                                "id"  : file_uuid,
                                "sha1": sha1_hash
                                }
                   }
        
        json_object = json.dumps(message)
        
        #TODO: encrypt object
        #TODO: send message
        
    def get_peers(self):
        message = {
                   "msg_type": "get_peers",
                   "params"  : {
                                "id": self.uuid
                                }
                   }
        
        json_object = json.dumps(message)
        
        #TODO: encrypt object
        #TODO: send message
        
    def get_grant_revoke_params(self, file_uuid, client_uuid, read, write):
        '''
        Preps message for either grant or revoke
        '''
        
        #ensure it's a string
        file_uuid = str(file_uuid)
        client_uuid = str(client_uuid)
        
        return {
                "id"    : file_uuid,
                "client": client_uuid,
                "read"  : read,
                "write" : write
                }
        
    def grant_rights(self, file_uuid, client_uuid, read, write):
        '''
        Grants rights on file to client
        '''
        
        params = self.get_grant_revoke_params(file_uuid, client_uuid, read, write)
        message = {
                    "msg_type":"grant",
                    "params":params
                   }
        
        json_object = json.dumps(message)
        print json_object #debug
        
        #TODO: encrypt object
        #TODO: send message
        
    def revoke_rights(self, file_uuid, client_uuid, read, write):
        '''
        Revoke rights on file from client
        '''
        
        params = self.get_grant_revoke_params(file_uuid, client_uuid, read, write)
        message = {
                    "msg_type":"revoke",
                    "params":params
                   }
        
        json_object = json.dumps(message)
        print json_object #debug
        
        #TODO: encrypt object
        #TODO: send message
        
    def query_rights(self, file_uuid):
        '''
        Query rights of other users on a file
        '''
        
        file_uuid = str(file_uuid)
        
        message = {
                   "msg_type": "query_rights",
                   "params"  : {
                                "id": file_uuid
                                }
                   }
        
        json_object = json.dumps(message)
        
        #TODO: encrypt object
        #TODO: send message
        
    def list_files(self):
        '''
        Query for a list of files user has access to
        '''
        
        message = {
                   "msg_type":"list_files",
                    "params":self.uuid
                   }
        
        json_object = json.dumps(message)
        
        #TODO: encrypt
        #TODO: send
        
    def version_downloaded(self, sha1_hash):
        message = {
                   "msg_type": "version_downloaded",
                   "params"  : {
                                "id"  : self.uuid,
                                "sha1": sha1_hash
                                }
                   }
        
        json_object = json.dumps(message)
        
        #TODO: encrypt
        #TODO: send
        
    class MessageTask(threading.Thread):
        def run(self):
            '''
            This method will handle the sending/receiving of messages
            '''
            pass
        
        #TODO: implement send and receive
        
if __name__ == '__main__':
    '''
    Purely a testing method - should be removed before deploying
    '''
    client = Client()
    client.add_or_mod_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf")
    
    #num_slices = int(math.ceil(os.path.getsize("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf") / float(client.CONST_SLICE_SIZE)))
    #slices = list()
    #for x in range(num_slices):
    #    slices.insert(x, client.slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", x))
    client.send_slice(client.slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", 10), uuid.uuid4(), 10)
    #client.reassemble_slices(slices, "C:/Users/Justin/Desktop/trying/copy.pdf")
    pass
