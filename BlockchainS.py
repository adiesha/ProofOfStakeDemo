import logging
import os
import pickle
import random
import socket
import threading

import rsa

from BlockS import BlockS
from RaftS import RaftS, Persist
from TransactionS import TransactionS


class BlockchainS:
    def __init__(self, id):
        self.clientid = id
        self.last = None
        self.first = None
        self.data = {}
        self.map = None
        self.lock = threading.Lock()
        self.publickey = None
        self.privatekey = None
        self.verifierReward = 10
        self.ownershipMap = {}
        self.raft = None

    def __str__(self):
        return "Data {0}".format(self.data)

    def createAblock(self, transactions):
        minerinfo = self.raft.getNextMiner(self.getLastBlockNumber() + 1)
        if minerinfo is None:
            print("cannot continue. cannot find the next miner")
            return
        else:
            print(minerinfo)
        if minerinfo[0] == self.clientid:
            print("Can continue to create the block")
        else:
            print("Not my turn to create the block")
            return

        nextblockid = minerinfo[1]
        if nextblockid != self.getLastBlockNumber() + 1:
            print("Next block number is not the correct next block. Current bid: {0} nextblockid {1}".format(
                self.getLastBlockNumber(), nextblockid))
            return False

        block = BlockS(nextblockid)
        block.addTransactions(transactions)
        block.miner = self.clientid
        if self.last is not None:
            block.prevhash = self.last.hash
        block.coinbase = (self.clientid, 100)
        block.hash = block.getHash()

        self.extractData()
        if self.validateBlock(block) and self.validate(block):
            print("block validated")
        else:
            print("Random transactions has a conflict try again")
            return False

        # this does not mean it is committed, we need to look at the signatures
        self.hookNewBlock(block)
        self.sendMessage(block, "hook")

        # commit the message
        self.commitBlock()

        # newblockid = self.getLastBlockId() + 1
        # block = BlockS(newblockid)
        # block.addTransactions(transactions)
        # # add prev block hash
        # if self.last is not None:
        #     block.prevhash = self.last.hash
        # block.coinbase = (self.clientid, 100)
        #
        # self.lock.acquire()
        # self.extractData()
        # if self.validateBlock(block) and self.validate(block):
        #     self.addNewBlockToChain(block)
        # else:
        #     print("Random transactions has a conflict try again")
        # self.lock.release()
        # self.sendMessage()

    def hookNewBlock(self, block):
        self.hook = block

    def getLastBlockId(self):
        if self.last is None:
            return 0
        else:
            return self.last.id

    def validateBlock(self, block, data=None):
        if data is None:
            data = self.data

        for tr in block.transactions:
            sender = tr.sender
            receiver = tr.recipient
            amount = tr.amount
            if sender in data:
                if data[sender] >= amount:
                    pass
                else:
                    print("Does not have enough balance in sender {0} Amount:{1}".format(sender, data[sender]))
                    return False
            else:
                print("Sender not in the block chain {0}".format(sender))
                return False
        return True

    def addNewBlockToChain(self, block):
        temp = self.last
        if temp is not None:
            temp.next = block
        self.last = block
        self.last.prev = temp

        self.hook = None
        self.last.committed = True
        if self.first is None:
            self.first = self.last

    def validate(self, block):
        # hash the block and check whether it matches with the hash inside it.
        if block.hash == block.gethash():
            print("Block id {0} hash is correct".format(block.id))
        else:
            print("Incorrect hash in the block {0} vs {1}".format(block.hash, block.gethash()))
            return False

        # check the previous hash
        # search the previous block and find it
        prevblock = self.getBlock(block.id - 1)
        if prevblock is None:
            if block.id == 1:
                if block.prevhash == ''.join('0' for i in range(64)):
                    return True
            else:
                print(
                    "block cannot be validated because blockchain does not have its previous block and block is not the initial block")
                print("Block id {0} Block hash {1} block's prev hash {2}".format(block.id, block.hash, block.prevhash))
                return False
        if prevblock.hash == block.prevhash:
            print("Block is validated against its hash and its previous blocks hash")
            return True
        else:
            print("Previous hash is incorrect {0} vs {1}".format(prevblock.hash, block.prevhash))
            return False

    def getBlock(self, id):
        temp = self.last
        if temp is None:
            return None
        else:
            while temp is not None:
                if id == temp.id:
                    return temp
                else:
                    temp = temp.prev
            return None

    def printChain(self):
        temp = self.last
        while (True):
            if temp is None:
                return
            else:
                print(temp)
                temp = temp.prev

    def extractData(self):
        temp = []
        block = self.last
        while block is not None:
            temp.append(block)
            block = block.prev
        # print(temp)

        data = {}
        self.ownershipMap = {}
        while temp:
            bl = temp.pop()
            if not bl.committed:
                continue
            # calculate the ownership percentages
            if bl.miner in self.ownershipMap:
                self.ownershipMap[bl.miner] = self.ownershipMap[bl.miner] + 1
            else:
                self.ownershipMap[bl.miner] = 1

            coinbase = bl.coinbase
            if coinbase[0] is not None:
                if coinbase[0] in data:
                    data[coinbase[0]] = data[coinbase[0]] + coinbase[1]
                else:
                    data[coinbase[0]] = coinbase[1]

            for tr in bl.transactions:
                sender = tr.sender
                receiver = tr.recipient
                amount = tr.amount
                if sender in data:
                    data[sender] = data[sender] - amount
                else:
                    print("Error: No sender in the data")
                    return None
                if receiver in data:
                    data[receiver] = data[receiver] + amount
                else:
                    data[receiver] = amount
            # print(data)

            for sign in bl.signatures:
                id = sign[0]
                if id in data:
                    data[id] = data[id] + self.verifierReward
                else:
                    data[id] = self.verifierReward
            self.data = data
        print(self.data)

    def createSetOfTransacations(self):
        temptr = []
        if self.data is None:
            return []
        else:
            noOftransactions = random.randint(1, 10)
            print("Selected transaction amount {0}".format(noOftransactions))
            keys = list(self.data.keys())
            while (noOftransactions > 0 and keys):
                choice = random.choice(keys)
                keys.remove(choice)
                sendersbalance = self.data[choice]
                amount = random.randint(0, sendersbalance)
                tr = TransactionS()
                tr.sender = choice
                tr.recipient = random.randint(0, 10)
                tr.amount = amount
                noOftransactions -= 1
                temptr.append(tr)

            return temptr

    def sendMessage(self, message, type):
        strReq = {}
        strReq['id'] = self.clientid
        strReq['msg'] = message
        strReq['type'] = type
        pickledMessage = pickle.dumps(strReq)
        for k, v in self.map.items():
            if k != self.clientid:
                self.sendViaSocket(k, pickledMessage)

    def sendViaSocket(self, k, m):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((self.map[k][0], int(self.map[k][1])))
                s.sendall(m)
                data = self.receiveWhole(s)
                # print(data)
                print(pickle.loads(data))

                self.receiveMessage(pickle.loads(data))

            except ConnectionRefusedError:
                print("Connection cannot be established to node {0}".format(k))
                logging.error("Connection cannot be established to node {0}".format(k))

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

    def receiveMessage(self, message):
        nid = message['id']
        msg = message['msg']
        type = message['type']

        print("Block id {0}".format(msg.id))
        print("Node id {0}".format(nid))
        # if the block id already exist in the bc reject
        currentid = 0 if self.last is None else self.last.id
        if currentid >= msg.id:
            print("New block already exist. current last block {0}: Received block {1}".format(self.last, msg))
            return [False, "New block already exist. current last block {0}: Received block {1}".format(self.last, msg)]
        # validate the block for double spending else reject
        self.extractData()

        if type == 'hook':
            # validate the block and send the signature
            if self.validate(msg) and self.validateBlock(msg):
                print("validated")
                signature = self.addSignature(msg)

                return [True, "Block validated", (self.clientid, signature)]
            else:
                print("not validated")
                return [False, "block not validated"]

        elif type == 'reply':
            nd, signature = message['signature']
            assert nd == nid
            result = self.validateSignature(msg.hash, signature, nid)
            if result:
                print("Reply signature was validated for block id: {0} by node {1}".format(msg.id, nid))

                # check whether it is already committed
                if self.getBlock(msg.id) is not None and self.getBlock(msg.id).committed:
                    print("Block was already committed, therefore signature will not be added")
                    return True, "Block was already committed, therefore signature will not be added"

                self.hook.addSign((nid, signature))
                # if number of signatures are more than the majority then accept
                # number of signatures = len(self.hook.signatures) + 1 for the miner
                noOfconsensus = 1 + len(self.hook.signatures)
                if noOfconsensus >= self.getSimpleMajority():
                    print("commit block: Number of consensus {0}".format(noOfconsensus))
                    self.addNewBlockToChain(self.hook)
                    return [True, "Block was committed"]
                else:
                    return [True, "Reply signature was validated for block id: {0} by node {1}".format(msg.id, nid)]

            else:
                print("Invalid signature was received as a reply for block id: {0} by node {1}".format(msg.id, nid))
                return [False,
                        "Invalid signature was received as a reply for block id: {0} by node {1}".format(msg.id, nid)]
        elif type == 'commit':
            self.addNewBlockToChain(msg)
            return [True, "Added to the blocl"]
        else:
            print("Incorrect type")
            return [False, "Incorrect request type"]

        return

        # find the latest block that is common with the list and the bc
        currenttemp = self.last
        filter = []
        while currenttemp is not None:
            filter = [b for b in lst if b.hash == currenttemp.hash]
            if filter:
                break
            else:
                currenttemp = currenttemp.prev

        self.lock.acquire()
        if filter:
            currentBlock = filter[0]
            tempid = currentBlock.id + 1
            self.last = currentBlock
        else:
            self.last = None
            tempid = 1
        while tempid <= msg.id:
            # find the block from the list
            NextBlock = [b for b in lst if b.id == tempid]
            blnew = NextBlock[0]
            self.extractData()
            if not self.validateBlock(blnew):
                print("New block validation failed. Not going to add it to the blockchain Block Id {0}".format(
                    blnew.id))
                return False, "New block validation failed. Not going to add it to the blockchain Block Id {0}".format(
                    blnew.id)
            else:
                # validate the block hashes
                if not self.validate(blnew):
                    print("Block hashes does not match")
                    return False, "Block hashes does not match"
                else:
                    print("validation successful for block {0}".format(blnew))
                    self.addNewBlockToChain(blnew)

            tempid += 1
        self.lock.release()

    def addSignature(self, block):
        hs = block.gethash()
        signature = rsa.sign(hs.encode(), self.privatekey, 'SHA-256')
        block.addSign((self.clientid, signature))
        return signature

    def validateSignature(self, message, signature, verifierid):
        messageEncoded = message.encode()
        publickkeyofverifier = self.map[verifierid][2]
        return rsa.verify(messageEncoded, signature, publickkeyofverifier) == 'SHA-256'

    def getOwnershipPercentage(self, id):
        if self.last is None:
            return 0.0
        nofBlocks = self.last.id
        noOfblocksownedbyID = self.ownershipMap[id]
        ratio = noOfblocksownedbyID / nofBlocks
        print("Percentage of block owned by ID: {0} is {1}".format(id, ratio))
        return ratio

    def createRaftServerAndInitializeRaft(self):
        raft = RaftS(1)
        raft.HOST = None
        raft.id = self.clientid
        raft.clientip = self.map[self.clientid][0]
        raft.clientPort = self.map[self.clientid][1] + 1
        raft.mapofNodes = self.map
        print("creating the proxy map")
        raft._createProxyMap()

        currrent_dir = os.getcwd()
        finalpersistdir = os.path.join(currrent_dir, 'persist')
        if not os.path.exists(finalpersistdir):
            os.mkdir(finalpersistdir)

        raft._persist = Persist()
        raft._persist.updateNetworkInfo(raft.HOST, raft.clientip, raft.clientPort)
        raft._persist.updateNodeInfo(raft.id, raft.mapofNodes, raft.noOfNodes)
        raft._persist.updateCurrentInfo(0, None, [])
        _persist(raft._persist, raft.log)

        print(raft.map)
        raft.noOfNodes = len(raft.map)
        raft._createRPCServer()
        raft.createTimeoutThread()
        raft.bc = self
        self.raft = raft
        return raft

    def getLastBlockNumber(self):
        if self.last is None:
            return 0
        else:
            return self.last.id

    def updateTheownershipMap(self):
        # update the ownership map to zero if they don't have keys
        for k in self.map.keys():
            # if k not in self.ownershipMap:
            self.ownershipMap[k] = 0

        if self.last is None:
            return self.ownershipMap
        else:
            temp = self.last
            while temp is not None:
                self.ownershipMap[temp.miner] = self.ownershipMap[temp.miner] + 1
                temp = temp.prev

    def getSimpleMajority(self):
        if len(self.map) % 2 == 0:
            return int(len(self.map) / 2 + 1)
        else:
            return int(len(self.map) / 2) + 1

    def commitBlock(self):
        # check whether last block is not committed
        print("trying to commit")
        if self.last is not None:
            self.last.committed = True
            print("Commit message for block id: {0}".format(self.last.id))
            self.sendMessage(self.last, "commit")


def _persist(obj, log):
    try:
        file = open("persist/data{0}.pickle".format(obj.id), "wb")
        pickle.dump(obj, file)
        file.close()
    except Exception as e:
        print("Exception Occurred while accessing persistent storage {0}".format(e))

    if log and log is not None:
        try:
            print("Logging to readable storagre {0}".format(obj, id))
            copy = log.copy()
            file = open("log/log-readable{0}.txt".format(obj.id), "w")
            for e in copy:
                file.write(str(e) + "\n")
            file.close()
        except Exception as e:
            print("Exception occurred while persisting readable log {0}".format(e))
