import random


class TransactionS:
    def __init__(self):
        self.amount = 0
        self.sender = None
        self.recipient = None

    def __str__(self):
        return "$: {0} From: {1} ---> To: {2} ".format(self.amount, self.sender, self.recipient)

    def createTestTransaction(self, sender=1):
        self.amount = random.randint(0, 200)
        self.sender = sender
        self.recipient = random.randint(0, 10)
