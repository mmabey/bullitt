#!/usr/bin/python

'''
Created on Apr 4, 2012

@author: Justin

'''

# Library imports
import base64
import hashlib
import json
import math
import os
import Queue
import subprocess
import threading
import time
import uuid #use str(uuid.uuid4())

# Third-party libraries
import Crypto
import pika

from pika.credentials import PlainCredentials

# Local imports
from bullitt.common import cuffrabbit

# Constants/Globals
DEBUG = False
VERBOSE = False
SESSION_KEY_LENGTH = 32
AES_BLOCK_SIZE = 32
PAD_CHAR = '*'
END_JSON_CHAR = '}'
#cuffrabbit.DEBUG = True
#cuffrabbit.INFO = True


class Client():

    def __init__(self):
        #read slice size from our config json
        client_dir = os.path.dirname(os.path.realpath(__file__))
        #TODO: return this line to normal
        #with open(os.path.join('/home/vlab/keypair', 'client.json')) as fin:
        with open(os.path.join('./setup', 'client1.json')) as fin:
            cconfig = json.load(fin)
            self.uuid = cconfig['uuid']
            self.ipaddr = cconfig['ip']
            self.pri_key = cconfig['pri_key']
            self.pub_key = cconfig['pub_key']
        
        common_dir = os.path.realpath(os.path.join(client_dir, '..', 'common'))
        with open(os.path.join(common_dir, 'gen_config.json')) as fin:
            json_data = json.load(fin)
        self.CONST_SLICE_SIZE = json_data["slice_size"]
        self.rabbit_server = str(json_data['rabbit_server'])
        self.server_queue = str(json_data['op_queue'])
        exchange = json_data['server_exchange']
        
        # Keep session keys in this
        self.session_keys = dict(server='')
        self.public_keys = dict()
        
        # Use this for long-term storage of file info.
        # Keep a structure of {file_id: [name, size, sha1]}
        self.file_info = {}

        #initialize queues
        self.out_queue = Queue.Queue()
        self.op_resp_queue = Queue.Queue()
        self.file_slice_queue = Queue.Queue()
        
        #initialize messengers
        self.sender = _Sender(self.rabbit_server, self.out_queue, self.uuid)
        self.sender.exchange = exchange
        self.receiver = _Receiver(self, self.rabbit_server, self.uuid,
                                  self.op_resp_queue, self.file_slice_queue)
        self.receiver.exchange = exchange
        
        self.sender.start()
        self.receiver.start()
        
        if VERBOSE:
            print "Sender thread %s alive" % (self.sender.isAlive() \
                                              and 'IS' or 'is NOT')
            print "Receiver thread %s alive" % (self.receiver.isAlive() \
                                                and 'IS' or 'is NOT')
 
        self.received_in_progress = dict()       
        self.requested_file_slice_count = dict()
        self.expect_slices = dict()


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

    def send_new_key(self, client_id, encrypted_aes_key):
        self.send_op_msg('new_key', client=client_id, key=encrypted_aes_key)

    def encrypt_data(self, data, client_id):
        '''
        Encrypt some data using a given key
        '''
        try: #AES
            
            key = self.session_keys[client_id]
            ciphertext = self.encrypt_data_aes(data, key)    
        except KeyError: #RSA
            #generate session key
            self.create_session_key(client_id)
            key_ciphertext = self.encrypt_data_rsa(self.session_keys[client_id], \
                                               self.public_keys[client_id])
            #TODO: send key_ciphertext in a json message
            #TODO: request rsa pubkey from server if you dont have it
            ciphertext = self.encrypt_data_aes(data, \
                                               self.session_keys[client_id])
        
        return ciphertext
    
    def encrypt_data_rsa(self, data, key):
        from Crypto.PublicKey import RSA
        
        encryptor = RSA.importKey(key)

        ciphertext = encryptor.encrypt(data, 0)

        return ciphertext
    
    def encrypt_data_aes(self, data, key):
        from Crypto.Cipher import AES
        
        encryptor = AES.new(key)
          
        data = self.pad_aes_data(data)
              
        ciphertext = encryptor.encrypt(data)
        
        return ciphertext
    
    def pad_aes_data(self,data):
        '''
        add padding so the block size is proper
        '''
        pad_count = AES_BLOCK_SIZE - (len(data) % AES_BLOCK_SIZE)
        
        pad = PAD_CHAR * pad_count
        
        padded = data + pad
        
        return padded
    
    def unpad_json_aes_data(self, data):
        '''
        remove any padding after the final } in a json message
        '''
        end_msg = data.rfind(END_JSON_CHAR) + 1
    
        return data[0:end_msg]
    
    def decrypt_data(self, data, other_party):
        '''
        Decrypt slice
        '''
        #Use session key to decrypt via Crypto
        #This may be changed to decrypt the entire JSON message instead
        #In which case this will be called from the receive method
        #instead of the reassembly 

        try:
            key = self.session_keys[other_party]
            return self.decrypt_data_aes(data, key)
        except KeyError: #save the session key
            decrypted = self.decrypt_data_rsa(data)
            #TODO: finish this logic
            # need to dump the json and retrieve the key from there
            self.session_keys[other_party] = decrypted
        
    def decrypt_data_rsa(self, data):
        from Crypto.PublicKey import RSA
        
        decryptor = RSA.importKey(self.pri_key)
        plaintext = decryptor.decrypt(data)
        
        return plaintext
    
    def decrypt_data_aes(self, data, key):
        from Crypto.Cipher import AES
        
        decryptor = AES.new(key)
        
        plaintext = decryptor.decrypt(data)
        
        plaintext = self.unpad_json_aes_data(plaintext)
        
        return plaintext

    def create_session_key(self, other_party):
        '''
        Create a session key
        '''       
        from Crypto import Random
        
        #grab the RNG
        rnd = Random.OSRNG.posix.new()
        #Get a key
        key = rnd.read(SESSION_KEY_LENGTH)
        #store it in the list
        self.session_keys[other_party] = key
    
    def send_slice(self, slice, file_uuid, slice_num):
        '''
        Send slice to a vm
        
        Slices are encoded as base64 for transfer (JSON doesnt handle binary)
        '''
        
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
        self.out_queue.put(json_object)
    
    
    #TODO: modified to present correlation_id
    def send_op_msg(self, msg_type, bytes=None, client=None, id=None,
                    name=None, num=None, prev_sha1=None, read=None, sha1=None,
                    slice=None, write=None, correlation_id=None):
        '''
        Send a generic client operation message to the server.
        
        Parameters:
        msg_type = REQUIRED. Operation type specified by the message
        bytes = Size of the file
        client = The grantee client's UUID
        id = The file UUID
        name = The file name
        num = Slice number
        prev_sha1 = SHA1 of the previous version of the file
        read = True/False/None read permission
        sha1 = SHA1 of the current version of the file
        slice = The data of the slice
        write = True/False/None write permission
        session_uuid = uuid used for identifying session keys
        key = session key
        correlation_id = who a a session is with
        '''
        to_pop = []
        params = locals()
        
        # Parameters not inserted into the message body under "params"
        params.pop('self') # Don't need to send a reference to ourselves
        params.pop('msg_type') # Added separately to body
        params.pop('correlation_id') # Added to parameters, not the msg body
        
        # Remove any unnecessary keys (None value)
        for key in params:
            # Can't change the dictionary while iterating over it
            if params[key] is None: to_pop.append(key)
        for key in to_pop: 
            params.pop(key)
        body = self.encrypt_data(json.dumps(dict(msg_type=msg_type,
                                                 params=params)),
                                 self.session_keys['server'])
        self.out_queue.put((self.server_queue, body, correlation_id))
    
        
    def add_or_mod_file(self, filename, prev_sha1=None, file_uuid=None):
        '''
        Sends a file_handle to the server
        Including a previous hash and uuid implies an update
        '''
        # Read data in as binary and generate SHA1
        with open(filename, "rb") as fin:
            sha1_hash = hashlib.sha1(fin.read()).hexdigest()
        
        # Grab file size
        file_size = os.path.getsize(filename)
        
        # If this is a new file, create a UUID for new file_handle
        if file_uuid == None:
            file_uuid = str(uuid.uuid4())
        
        msg_type = prev_sha1 is not None and 'mod_file' or 'add_file'
        self.send_op_msg(msg_type, id=file_uuid, name=filename,
                         bytes=file_size, sha1=sha1_hash, prev_sha1=prev_sha1)
    
    
    def delete_file(self, file_uuid, sha1_hash):
        '''
        Send message to delete file from server
        '''
        self.send_op_msg('del_file', id=file_uuid, sha1=sha1_hash)
    
    
    def get_peers(self, file_uuid):
        self.send_op_msg('get_peers', id=file_uuid)
    
    
    def grant_rights(self, file_uuid, client_uuid, read, write):
        '''
        Grants rights on file to client
        '''
        self.send_op_msg('grant', id=file_uuid, client=client_uuid,
                         read=read, write=write)
    
    
    def revoke_rights(self, file_uuid, client_uuid, read, write):
        '''
        Revoke rights on file from client
        '''
        self.send_op_msg('revoke', id=file_uuid, client=client_uuid,
                         read=read, write=write)
    
    
    def query_rights(self, file_uuid):
        '''
        Query rights of other users on a file
        '''
        self.send_op_msg('query_rights', id=str(file_uuid))
    
    
    def list_files(self):
        '''
        Query for a list of files user has access to and return it.
        '''
        while True:
            self.send_op_msg('list_files')
            try:
                props, action, params = self.get_op_resp()
            except Queue.Empty:
                print "Unable to retrieve message. Response timed out."
                break
            #TODO: Do something with the returned result before returning?
            if action == 'files_list':
                return params['files']
            # This is not the action you are looking for. Move along.
            if DEBUG:
                print "Potentially entering (or already in) infinite loop... " \
                      "Can't stop..."
            self.oops((props, action, params))
    
    
    def version_downloaded(self, file_uuid, sha1_hash):
        self.send_op_msg('version_downloaded', id=file_uuid, sha1=sha1_hash)
    
    
    def request_file(self, file_uuid, sha1_hash, bytes):     
        
        session_id = uuid.uuid4()
         
        self.send_op_msg('request_file', id=str(file_uuid), sha1=sha1_hash, \
                         correlation_id=session_id)
        
        self.requested_file_slice_count[file_uuid] = int(
                            math.ceil(bytes / float(client.CONST_SLICE_SIZE)))
        
        self.expect_slices[file_uuid] = dict()
    
    
    def get_op_resp(self):
        '''
        Block until a response for an operation is received.  Use with caution!
        '''
        resp = self.op_resp_queue.get(timeout=5)
        self.op_resp_queue.task_done()
        return resp
    
    
    def oops(self, val):
        '''
        Put something back on the queue that wasn't supposed to leave it.
        
        Could be the entrance to an infinite loop...  Oops.
        '''
        self.op_resp_queue(val)
        time.sleep(1)



class _Sender(cuffrabbit.RabbitObj, threading.Thread):
    '''
    Based on Mike's code in db.py
    '''
    
    def __init__(self, host, queue, user_id):
        '''
        '''
        # Initialize thread
        threading.Thread.__init__(self)
        self.daemon = True
        
        # Initialize connection parameters
        self.user_id = user_id
        creds = PlainCredentials(self.user_id, 'pika')
        cuffrabbit.RabbitObj.__init__(self, **dict(host=host,
                                                   credentials=creds))
        self.output_queue = queue
    
    
    def run(self):
        if VERBOSE: print "[i] Initiating sender connection with server..."
        # Connect to MQ server. Should be last thing in this method.
        self.init_connection(callback=self.start_sending,
                             exchange=self.exchange, exchange_type='direct',
                             routing_key=self.user_id)
    
    
    def start_sending(self, stupid):
        '''
        Wait for a message to be put in the output queue and send it.
        
        The parameter "stupid" is only required for pika to not complain.
        '''
        while True:
            if VERBOSE: print " :) Ready to send messages"
            queue, msg, correlation_id = self.output_queue.get()
            if DEBUG: 
                print "Sending message to '%s' the following:\n%r" % (queue,
                                                                      msg)
            self.send_message(msg, routing_key=queue,
                              correlation_id=correlation_id)
                                                             
            # Signal the queue that the message has been sent
            self.output_queue.task_done()



#TODO: implement receive
class _Receiver(cuffrabbit.RabbitObj, threading.Thread):
    '''
    Receiver based on Mike's code
    '''
    
    def __init__(self, parent, host, user_id, op_resp_queue, file_slice_queue):
        '''
        '''
        # Initialize thread
        threading.Thread.__init__(self)
        self.daemon = True
        
        # Initialize connection parameters
        self.user_id = user_id
        creds = PlainCredentials(self.user_id, 'pika')
        cuffrabbit.RabbitObj.__init__(self, **dict(host=host,
                                                   credentials=creds))
        self.op_resp_queue = op_resp_queue
        self.file_slice_queue = file_slice_queue
        self.parent = parent
    
    
    def run(self):
        if VERBOSE: print "[i] Initiating receiver connection with server..."
        # Connect to MQ server. Should be last thing in this method.
        self.init_connection(callback=self.main, queue_name=self.user_id,
                             exchange=self.exchange, exchange_type='direct',
                             routing_key=self.user_id)
    
    
    def main(self, stupid):
        '''
        '''
        if VERBOSE: print " :) Listening on queue %s" % self.user_id
        # Start listening to the queue
        self.receive_message(callback=self.process_msg)
        
        # Do anything else that should be asynchronous to listening for messages
    
    
    def process_msg(self, ch, method, props, body):
        '''
        If the message from the client is valid, perform the requested action.
        
        Since this method is specified as the callback whenever a message is
        received from a client, 
        '''
        # Extract client's ID and check it is valid
        # MIKE: If the server originated this message, the client ID will not
        # be "valid" and will likely be None. What were you trying to do here 
        # exactly?
        client_id = props.user_id
        
        #TODO: decrypt slice message
        
        # Parse message
        job_data = json.loads(body)
        
        action = job_data['msg_type']
        params = job_data['params']
        if VERBOSE: print "Got message: '%s'\n%r" % (action, params)
        
        # Perform requested action
        #TODO: add the other operations here
        if action == 'send_slice':
           
            #TODO: decrypt into var json_object
            
            json_object = None #decrypt the message here
            
            id = json_object['id']
            num = json_object['num']
            slice = base64.b64decode(json_object['slice'])
            
            self.parent.received_in_progress[id] = list()
            handle = self.parent.received_in_progress[id]
            handle[num] = slice
            
            if len(handle) == self.parent.requested_file_slice_count:
                #TODO: fix the id in the next line - should be filename
                self.parent.reassemble_slices(handle, id)
                del self.parent.received_in_progress[id]
        else:
            self.op_resp_queue.put((props, action, params))
        
        # Acknowledge message
        self.ack(method.delivery_tag)


if __name__ == '__main__':
    '''
    Purely a testing method - should be removed before deploying
    '''
    client = Client()
    
    client.public_keys["bob"] = client.pub_key
    
    ciphert = client.encrypt_data("a", "bob")
    
    plain = client.decrypt_data(ciphert, "bob")
    
    print plain
    
    print json.dumps(plain)
    
    #client.add_or_mod_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf")
    
    #num_slices = int(math.ceil(os.path.getsize("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf") / float(client.CONST_SLICE_SIZE)))
    #slices = list()
    #for x in range(num_slices):
    #    slices.insert(x, client.slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", x))
    #client.send_slice(client.slice_file("C:/Users/Justin/Desktop/trying/Paper-5(1).pdf", 10), uuid.uuid4(), 10)
    #client.reassemble_slices(slices, "C:/Users/Justin/Desktop/trying/copy.pdf")
    pass
