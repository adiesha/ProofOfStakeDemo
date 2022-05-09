import hashlib
import time


class BlockS:
    def __init__(self, id):
        self.id = id
        self.miner = None
        self.transactions = []
        self.prev = None
        self.next = None
        self.prevhash = ''.join('0' for i in range(64))
        self.hash = None
        self.trasactionstring = None
        self.coinbase = (None, 0)
        self.signatures = []
        self.committed = False

    def gethash(self):
        text = self.gethashablestring()
        encodedText = text.encode()
        temphash = hashlib.sha256(encodedText).hexdigest()
        return temphash

    def gethashablestring(self):
        blockid = str(self.id)
        coinbase = str(self.coinbase)
        transactiondata = self.getTransactionsStringFromcache()
        prev = self.prevhash
        result = blockid + coinbase + transactiondata + prev
        return result

    def getHash(self):
        text = self.gethashablestring()
        encodedText = text.encode()
        temphash = hashlib.sha256(encodedText).hexdigest()
        return temphash

    def getTransactionsStringFromcache(self):
        if self.trasactionstring is None:
            self.trasactionstring = self.createTransactionDataString()
            return self.trasactionstring
        else:
            return self.trasactionstring

    def createTransactionDataString(self):
        transactiondata = ''
        for t in self.transactions:
            transactiondata = transactiondata + str(t)
        return transactiondata

    def addTransaction(self, tr):
        self.transactions.append(tr)

    def addTransactions(self, trarray):
        self.transactions = trarray

    def printTransactions(self):
        print("Coin base: {0} -> {1}".format(self.coinbase[1], self.coinbase[0]))
        for t in self.transactions:
            print(t)

    def addSign(self, sign):
        self.signatures.append(sign)


    def __str__(self):
        return "BlockID: {0} CoindBase: {1} Tr: {2} PreviousHash : {3} Hash:{4}".format(self.id,
                                                                                        self.coinbase,
                                                                                        self.transactions,
                                                                                        self.prevhash,
                                                                                        self.hash)
