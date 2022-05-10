# echo-client.py
import json
import logging
import os
import pickle
import random
import socket
import sys
import threading
import time

import rsa

from BlockchainS import BlockchainS


class ClientS():

    def __init__(self, host="127.0.0.1", clientip="127.0.0.1", serverport=65431, hb=20, toggleHB=False):
        self.HOST = host  # The server's hostname or IP address
        self.SERVER_PORT = serverport  # The port used by the server
        self.clientip = clientip
        self.clientPort = None
        self.seq = None
        self.map = None
        self.bc = None
        self.heartbeatInterval = hb
        self.togglehb = toggleHB
        self.clientmutex = threading.Lock()
        self.publickey = None
        self.privateKey = None

    def createJSONReq(self, typeReq, nodes=None, slot=None):
        # Initialize node
        if typeReq == 1:
            request = {"req": "1"}
            return request
        # Send port info
        elif typeReq == 2:
            request = {"req": "2", "seq": str(self.seq), "port": str(self.clientPort), "n": self.publickey.n,
                       "e": self.publickey.e}
            return request
        # Get map data
        elif typeReq == 3:
            request = {"req": "3", "seq": str(self.seq)}
            return request
        else:
            return ""

    def receiveWhole(self, s):
        BUFF_SIZE = 4096  # 4 KiB
        data = b''
        while True:
            part = s.recv(BUFF_SIZE)
            data += part
            if len(part) < BUFF_SIZE:
                # either 0 or end of data
                break
        return data

    def getJsonObj(self, input):
        jr = json.loads(input)
        return jr

    def initializeTheNode(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(1)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            self.seq = int(resp['seq'])
            print("sequence: " + str(self.seq))
            s.close()
        currrent_dir = os.getcwd()
        finallogdir = os.path.join(currrent_dir, 'log')
        if not os.path.exists(finallogdir):
            os.mkdir(finallogdir)
        logging.basicConfig(filename="log/{0}.log".format(self.seq), level=logging.DEBUG, filemode='w')

    def sendNodePort(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(2)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            print(resp['response'])
            s.close()

    def downloadNodeMap(self):
        # establish connection with server and give info about the client port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(3)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            print(resp)
            s.close()
            return resp

    def getMapData(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(3)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))
            resp2 = {}
            for k, v in resp.items():
                pkey = rsa.key.PublicKey(v[2], v[3])
                resp2[int(k)] = (v[0], int(v[1]), pkey)

            print(resp2)
            s.close()
            return resp2

    def process(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, int(self.clientPort)))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    time.sleep(2)
                    while True:
                        data = self.receiveWhole(conn)
                        if data == b'':
                            break
                        message = self.getJsonObj(data.decode("utf-8"))
                        if list(message.keys())[0] == "req":
                            event = message
                            print("Message received: ", message)
                        else:
                            self.map = message
                            print("Updated Map: ", self.map)

    def createThreadToListen(self):
        thread = threading.Thread(target=self.ReceiveMessageFunct)
        thread.daemon = True
        thread.start()
        return thread

    def ReceiveMessageFunct(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Started listening to clientip {0} port {1}".format(self.clientip, self.clientPort))
            s.bind((self.clientip, self.clientPort))
            while True:
                s.listen()
                conn, addr = s.accept()
                with conn:
                    # print(f"Connected by {addr}")
                    logging.debug(f"Connected by {addr}")
                    while True:
                        data = self.receiveWhole(conn)
                        self.clientmutex.acquire()
                        if data == b'':
                            break
                        unpickledRequest = pickle.loads(data)
                        # print(unpickledRequest)
                        logging.debug(unpickledRequest)
                        if isinstance(unpickledRequest, dict):
                            # join the partial log
                            # np = unpickledRequest['pl']
                            # mtx = unpickledRequest['mtx']
                            # nid = unpickledRequest['nodeid']
                            print(unpickledRequest)
                            nid = unpickledRequest['id']
                            msg = unpickledRequest['msg']
                            type = unpickledRequest['type']
                            # lst = unpickledRequest['list']
                            print(nid, msg, type)
                            result = self.bc.receiveMessage(unpickledRequest)

                            # self.bc.receiveMessage((np, mtx, nid))

                            # conflicts = self.dd.checkConflictingAppnmts()
                            # for c in conflicts:
                            #     self.dd.cancelAppointment((c.timeslot, c))

                            # create message receive event
                            # send the message success request
                            response = {"id": self.seq, "type": "reply", "response": result[0], "replymsg": result[1],
                                        'signature': result[2] if len(result) > 2 else None, "msg": msg}
                            # the_encoding = chardet.detect(pickle.dumps(response))['encoding']
                            # response['encoding'] = the_encoding
                            pickledMessage = pickle.dumps(response)
                            try:
                                conn.sendall(pickledMessage)
                            except:
                                print("Problem occurred while sending the reply to node {0}".format("jhgjy"))
                        else:
                            response = {"response": "Failed", "error": "Request should be a dictionary"}
                            # the_encoding = chardet.detect(pickle.dumps(response))['encoding']
                            # response['encoding'] = the_encoding
                            pickledMessage = pickle.dumps(response)
                            conn.sendall(pickledMessage)
                        self.clientmutex.release()
                        break

    def menu(self, bc, raft):
        while True:
            print("Display Blockchain\t[d]")
            print("Display Last Block\t[l]")
            print("Create new block\t[b]")
            print("Press e to print diagnostics")
            print("Quit    \t[q]")

            resp = input("Choice: ").lower().split()
            if len(resp) < 1:
                print("Not a correct input")
                continue
            if resp[0] == 'd':
                print("Display Blockchain")
                bc.printChain()
                raft._diagnostics()
                raft.printLog()
                # d.displayCalendar()
            elif resp[0] == 'l':
                print(bc.last)
            elif resp[0] == 'b':
                self.bc.createAblock(bc.createSetOfTransacations())
            elif resp[0] == 's':
                self.bc.sendMessage()
            elif resp[0] == 'q':
                print("Quitting")
                break
            elif resp[0] == 'e':
                self.bc.extractData()
            elif resp[0] == 'g':
                blockid = int(input("input the block id"))
                print(self.bc.getBlock(blockid))
            elif resp[0] == 'm':
                print(raft.getNextMiner(self.bc.getLastBlockNumber() + 1))
                print(self.bc.ownershipMap)
            elif resp[0] == 'n':
                raft.timeoutFlag = True
                raft.receivedHeartBeat = False
                raft.leaderTimeoutFlag = True
                raft.electionTimeoutFlag = False

    def main(self):
        print('Number of arguments:', len(sys.argv), 'arguments.')
        print('Argument List:', str(sys.argv))

        if len(sys.argv) > 1:
            print("Server ip is {0}".format(sys.argv[1]))
            self.HOST = sys.argv[1]
            print("Server Ip updated")

        if len(sys.argv) > 2:
            print("Client's ip is {0}".format(sys.argv[2]))
            self.clientip = sys.argv[2]

        else:
            print("User did not choose a client ip default is 127.0.0.1")
            self.clientip = "127.0.0.1"

        if len(sys.argv) > 3:
            print("user inputted client port {0}".format(sys.argv[3]))
            self.clientPort = int(sys.argv[3])
        else:
            print("User did not choose a port for the node. Random port between 55000-63000 will be selected")
            port = random.randint(55000, 63000)
            print("Random port {0} selected".format(port))
            self.clientPort = port

        self.initializeTheNode()
        self.createRSAKeys()
        self.sendNodePort()
        # need to put following inside the menu
        # self.createThreadToListen()
        print("Ready to start the Calendar. Please wait until all the nodes are ready to continue. Then press Enter")
        if input() == "":
            print("Started Creating the Blockchain and Raft Server")
            self.map = self.getMapData()
            blockchain = BlockchainS(self.seq)
            blockchain.map = self.map
            blockchain.privatekey = self.privateKey
            blockchain.publickey = self.publickey
            raft = blockchain.createRaftServerAndInitializeRaft()
            self.bc = blockchain
            # self.createThreadToListen()
            # self.createHeartBeatThread()
            self.createThreadToListen()
            self.menu(blockchain, raft)

    def createRSAKeys(self):
        self.publickey, self.privateKey = rsa.newkeys(512)


if __name__ == '__main__':
    client = ClientS()
    client.main()
