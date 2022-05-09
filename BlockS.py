import hashlib


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
        self.verifierReward = 10

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
        if type(sign) == tuple:
            self.signatures.append(sign)
        else:
            raise Exception("Signature is not a tuple")

    def getPrintableTransactionString(self):
        temp = ""
        for t in self.transactions:
            temp = temp + str(t) + "\n"

        temp = temp + "Verifier Rewards: \n"
        for sign in self.signatures:
            temp = temp + "$: {0} to {1}\n".format(self.verifierReward, sign[0])
        return temp

    def __str__(self):
        return "------------------------------------------------\nBlockID: {0} \nCoindBase: {1} \nTr: \n{2} \nPreviousHash : {3} \nHash:{4} \nminer : {5} \nNumber of Signatures: {6}\n------------------------------------------------\n".format(
            self.id,
            self.coinbase,
            self.getPrintableTransactionString(),
            self.prevhash[0:14],
            self.hash[0:14], self.miner, len(self.signatures))
