import json
import logging
import os
import pickle
import random
import socket
import sys
import threading
import time
from enum import Enum
from http.client import CannotSendRequest
from time import sleep
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import numpy as np


# ToDO: Mutex, commiting, threading, rpc threading, exceptions handling, basic input/output
#  adding function for clients (who is the leader?, persistent storage and restart, getting gamestate)
# add mutex to AddRequest

class RaftS:
    def __init__(self, id):
        # these attributes should be persistent
        self.id = id
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.map = None  # Map about the other nodes
        self.noOfNodes = None

        self.bc = None
        self.blockIdvsMiner = {}

        self.state = State.FOLLOWER
        self.leader = None

        # volatile states
        self.commitIndex = -1
        self.lastApplied = 0

        # volatile state on leader
        self.nextIndex = None
        self.matchIndex = None

        # check this flag every after every timeout
        # if false issue go for election
        # if not set this to false and choose another timeout and check after that time
        self.receivedHeartBeat = False

        self.mutexForAppendEntry = threading.Lock()
        self.mutexForHB = threading.Lock()
        self.voteMutex = threading.Lock()

        self.HOST = "127.0.0.1"
        self.clientip = "127.0.0.1"  # needs to be persisted
        self.clientPort = None  # needs to be persisted
        self.SERVER_PORT = 65431
        self.mapofNodes = None

        self.timeoutFlag = False
        self.leaderTimeoutFlag = False
        self.electionTimeoutFlag = False

        self._persist = None
        self.logMutex = threading.Lock()
        self.blue = None
        self.red = None

        self.gameMutex = threading.Lock()

        self.bstate = 0
        self.baction = 0
        self.bgotblocked = False
        self.blastpunchtime = 0

        self.rstate = 0
        self.raction = 0
        self.rgotblocked = False
        self.rlastpunchtime = 0

        self.normal_state = 0
        self.block_with_left_state = 1
        self.block_with_right_state = 2
        self.punch_with_left_action = 3
        self.punch_with_right_action = 4
        self.won = 5

        self.gameMessage = ""

    # This is the remote procedure call for leader to invoke in nodes
    # This is not the procedure call that does the appendEntries for leader
    def appendEntries(self, info):
        self.mutexForAppendEntry.acquire()
        # print("Acquiring mutex for appendEntry in node {0}".format(self.id))
        logging.debug("Acquiring mutex for appendEntry in node {0}".format(self.id))
        # First check whether this is a initial heartbeat by a leader.
        check = self.checkwhetheritsaheratbeat(info)
        if check is not None:
            self.mutexForAppendEntry.release()
            # print("Mutex for appendEntry is released in node {0}".format(self.id))
            logging.debug("Mutex for appendEntry is released in node {0}".format(self.id))
            return check, self.currentTerm
        else:
            if info['term'] < self.currentTerm:
                self.mutexForAppendEntry.release()
                # print("Mutex for appendEntry is released in node {0}".format(self.id))
                logging.debug("Mutex for appendEntry is released in node {0}".format(self.id))
                return False, self.currentTerm
            prevLogIndex = info['previouslogindex']
            prevLogTerm = info['previouslogterm']
            leadercommitIndex = info['leadercommit']
            # print("leaddercommmit {0}".format(leadercommitIndex))
            # print("prevLogIndex {0}".format(prevLogIndex))
            # print(info)
            if prevLogIndex == -1:
                pass
            else:
                print("prev:" + str(prevLogIndex))
                # if node is behind the leader prevlogindex may not be in the log
                print("len of log" + str(len(self.log)))
                if self.log[prevLogIndex].term != prevLogTerm:
                    # print("haha")
                    self.mutexForAppendEntry.release()
                    # print("Mutex for appendEntry is released in node {0}".format(self.id))
                    logging.debug("Mutex for appendEntry is released in node {0}".format(self.id))
                    print(
                        "Previous log term of the candidates {0} is not the same as the node's prevLogTerm {1}".format(
                            prevLogTerm, self.log[prevLogIndex].term))
                    return False, self.currentTerm

            # received actual appendEntry do the logic
            entries = info['values']
            for e in entries:
                print(e)
                print("Appending Entries in node {0} by the leader".format(self.id))
                print("Id of the entry that we are adding is {0}".format(e.id))
                logging.debug("Appending Entries in node {0} by the leader".format(self.id))
                logging.debug("Id of the entry that we are adding is {0}".format(e.id))
                if e.id != len(self.log):
                    print("Entry that we are adding is not at the correct log index {0} log length {1}".format(e.id,
                                                                                                               len(self.log)))
                    logging.debug(
                        "Entry that we are adding is not at the correct log index {0} log length {1}".format(e.id,
                                                                                                             len(self.log)))
                    # This needs to be double checked
                    self.log[e.id] = e
                    # raise Exception(
                    #     "Entry that we are adding is not at the correct log index {0} log length {1}".format(e.id,
                    #                                                                                          len(self.log)))
                else:
                    self.log.append(e)
                    self.commitIndex = leadercommitIndex
                    self.updateCommittedEntries()
            self._persist.updateCurrentInfo(self.currentTerm, self.votedFor, self.log)
            _persist(self._persist, self.log)
        self.mutexForAppendEntry.release()
        # print("Mutex for appendEntry is released in node {0}".format(self.id))
        logging.debug("Mutex for appendEntry is released in node {0}".format(self.id))
        return True, self.currentTerm

    def checkwhetheritsaheratbeat(self, info):
        # print(
        #     "Checking whether appendEntry is a Heartbeat from {0} to node {1} leader Term {2}".format(info['leaderid'],
        #                                                                                               self.id,
        #                                                                                               info['term']))
        logging.debug(
            "Checking whether appendEntry is a Heartbeat from {0} to node {1} leader Term {2}".format(info['leaderid'],
                                                                                                      self.id,
                                                                                                      info['term']))
        if info['values'] is None:
            info['values'] = []
        else:
            # print("Before")
            # print(info['values'])
            info['values'] = pickle.loads(info['values'].data)
            # print("After")
            # print(info['values'])
        # print(info['values'])
        if len(info['values']) == 0:
            if info['term'] < self.currentTerm:
                print("Term from the heartbeat is lower: HB Term {0} current term {1}".format(info['term'],
                                                                                              self.currentTerm))
                logging.debug("Term from the heartbeat is lower: HB Term {0} current term {1}".format(info['term'],
                                                                                                      self.currentTerm))
                return False
            else:
                # print("Heartbeat received by node {0} from the leader {1}".format(self.id, info['leaderid']))
                logging.debug("Heartbeat received by node {0} from the leader {1}".format(self.id, info['leaderid']))
                self.leader = info['leaderid']
                self.state = State.FOLLOWER
                self.mutexForHB.acquire()
                self.receivedHeartBeat = True
                self.mutexForHB.release()
                # print("Node {0} 's current Term is before update is {1} state {2}".format(self.id, self.currentTerm,
                #                                                                           self.state))
                logging.debug(
                    "Node {0} 's current Term is before update is {1} state {2}".format(self.id, self.currentTerm,
                                                                                        self.state))
                self.currentTerm = info['term']
                leaderPrevLogIndex = info['previouslogindex']
                if leaderPrevLogIndex > len(self.log):
                    print("Heartbeat was OK but node {0} is far behind the leader returning False")
                    return False
                self.commitIndex = info['leadercommit']
                self.updateCommittedEntries()
                # print("Node {0} 's current Term is updated to {1} state {2}".format(self.id, self.currentTerm,
                #                                                                     self.state))
                self._persist.updateCurrentInfo(self.currentTerm, self.votedFor, self.log)
                _persist(self._persist, self.log)
                return True
        else:
            print("AppendEntry from {0} to node {1} leader's Term {2} is not a HB".format(info['leaderid'], self.id,
                                                                                          info['term']))
            logging.debug(
                "AppendEntry from {0} to node {1} leader's Term {2} is not a HB".format(info['leaderid'], self.id,
                                                                                        info['term']))
            return None

    # Request RPC is the method that is invoked by a candidate to request the vote
    def requestVote(self, info):
        self.mutexForAppendEntry.acquire()
        # print('Acquiring mutex for request vote in node {0}'.format(self.id))
        # info is a dict: nodeid, term, lastindexofthelog lastlogterm
        candidateid = info['nodeid']
        term = info['term']
        candidateslastindexofthelog = info['lastindexofthelog']
        candidateslastlogterm = info['lastlogterm']

        if term < self.currentTerm:
            self.mutexForAppendEntry.release()
            # print('Releasing mutex for request vote in node {0}'.format(self.id))
            print("Request for vote was rejected for candidate {0} by node {0} for lower term".format(candidateid,
                                                                                                      self.id))
            logging.debug(
                "Request for vote was rejected for candidate {0} by node {0} for lower term".format(candidateid,
                                                                                                    self.id))
            return False
        elif term == self.currentTerm:
            print("candidates term {0} is equal to nodes {0} term: {2}".format(candidateid, self.id, term))
            logging.debug("candidates term {0} is equal to nodes {0} term: {2}".format(candidateid, self.id, term))
            if self.votedFor is None:
                pass
            else:
                print("Node {0} has already voted for {1} in term {2}".format(self.id, self.votedFor, self.currentTerm))
                logging.debug(
                    "Node {0} has already voted for {1} in term {2}".format(self.id, self.votedFor, self.currentTerm))
                self.mutexForAppendEntry.release()
                return False
        else:  # term >= self.currentTerm
            print("Node {0} term {1} is lower than the candidate {3} term {2}".format(self.id, self.currentTerm, term,
                                                                                      candidateid))
            print("Node {0}'s term is incremented to match the candidates term {1}".format(self.id, term))
            logging.debug(
                "Node {0} term {1} is lower than the candidate {3} term {2}".format(self.id, self.currentTerm, term,
                                                                                    candidateid))
            logging.debug("Node {0}'s term is incremented to match the candidates term {1}".format(self.id, term))
            self.currentTerm = term
            # updating votedFor to None since candidates term is larger than the current term
            self.votedFor = None

        # Need to implement this
        if self.votedFor is None:
            # Compare the term of the last entry against candidates last term
            # if they are the same then compare the last log index

            if candidateslastlogterm < self.getLastTerm():
                print("Candidates last term is smaller than the current term of the node {0}".format(self.id))
                logging.debug("Candidates last term is smaller than the current term of the node {0}".format(self.id))
                self.mutexForAppendEntry.release()
                # print('Releasing mutex for request vote in node {0}'.format(self.id))
                print(
                    "Request for vote was rejected for candidate {0} by node {0} for not having correct previous log "
                    "term")
                logging.debug(
                    "Request for vote was rejected for candidate {0} by node {0} for not having correct previous log "
                    "term")
                return False
            else:
                # candidates term is greater than or equal to nodes term, now we have to look at the last index
                if candidateslastlogterm == self.getLastTerm():
                    # check the last index of the log
                    if candidateslastindexofthelog < self.getLastIndex():
                        print(
                            "Candidates term and current term is equal but last index of the node {0} is greater than the candidates last index".format(
                                self.id
                            ))
                        logging.debug(
                            "Candidates term and current term is equal but last index of the node {0} is greater than the candidates last index".format(
                                self.id
                            ))
                        self.mutexForAppendEntry.release()
                        # print('Releasing mutex for request vote in node {0}'.format(self.id))
                        return False
                    else:
                        self.votedFor = candidateid
                        self._persist.updateCurrentInfo(self.currentTerm, self.votedFor, self.log)
                        _persist(self._persist, self.log)
                        self.mutexForAppendEntry.release()
                        # print('Releasing mutex for request vote in node {0}'.format(self.id))
                        return True
                else:  # candidates last log term is greater than the nodes last term therefore candidate is more updated
                    self.votedFor = candidateid
                    self._persist.updateCurrentInfo(self.currentTerm, self.votedFor, self.log)
                    _persist(self._persist, self.log)
                    self.mutexForAppendEntry.release()
                    # print('Releasing mutex for request vote in node {0}'.format(self.id))
                    return True
        else:  # Already voted for someone
            print(
                "Request rejected as node {0} has already voted for {1} in term {2}".format(self.id, self.votedFor,
                                                                                            self.currentTerm))
            logging.debug(
                "Request rejected as node {0} has already voted for {1} in term {2}".format(self.id, self.votedFor,
                                                                                            self.currentTerm))
            self.mutexForAppendEntry.release()
            # print('Releasing mutex for request vote in node {0}'.format(self.id))
            return False

    # # invoking appendEntries of other nodes
    # # This method should not be exposed to invoke
    # def _invokeAppendEntries(self):
    #     # check whether you are the leader
    #     if self.state != State.LEADER:
    #         print("Node {0} is not the leader cannot add entries".format(self.id))
    #         return
    #
    #     # add the entry to the log
    #     entry = Entry(len(self.log), self.currentTerm)
    #     self.log.append(entry)
    #     entry.id = len(self.log) - 1
    #
    #     info = self.createApppendEntryInfo()
    #
    #     info['value'] = pickle.dumps(entry)
    #
    #     # Node is the leader
    #     for k, v, in self.map.items():
    #         v.appendEntries(info)
    #     pass

    # # This method should invoke heartbeat function of other nodes
    # # This method should not be exposed to invoke
    # def _invokeHeartBeat(self):
    #     pass

    def _invokeRequestVoteRPV(self):
        print("Invoking Election by Node {0}".format(self.id))
        # vote for itself
        # loop the nodes and call the RequestVoteRPC
        vote = Vote()

        self.mutexForAppendEntry.acquire()
        self.state = State.CANDIDATE
        self.currentTerm = self.currentTerm + 1
        self.votedFor = self.id
        self.mutexForAppendEntry.release()
        vote.addVote()
        inf = self.createInfo()
        print(
            "Node:{0} Term: {1} state: {2} votedFor: {3}".format(self.id, self.currentTerm, self.state, self.votedFor))

        for k, v in self.map.items():
            if k != self.id:
                # result = self.createDaemon(v.requestVote, inf)
                self.requestVoteFromNode(k, v, inf, vote)

        if vote.getVotes() >= self.getSimpleMajority():
            print("Found the majority. Making node {0} the leader".format(self.id))
            self.state = State.LEADER
            self.intializevolatileStateOfTheLeader()
            # send a heartbeat
            return True
        else:
            print("Node {0} did not get the majority. Number of votes got is {1}".format(self.id, vote.votes))
            return False

    def intializevolatileStateOfTheLeader(self):
        print("Initializing volatile state of the leader")
        logging.debug("Initializing volatile state of the leader")
        if not self.log:
            self.nextIndex = np.zeros(self.noOfNodes, dtype=np.int32)
            self.matchIndex = np.zeros(self.noOfNodes, dtype=np.int32)
        else:
            lastIndex = self.getLastIndex()
            self.nextIndex = np.full(shape=self.noOfNodes, fill_value=lastIndex + 1, dtype=np.int32)
            self.matchIndex = np.full(shape=self.noOfNodes, fill_value=self.commitIndex, dtype=np.int32)

    def createDaemon(self, func, inf):
        thread = threading.Thread(target=func, args=(inf,))
        thread.daemon = True
        thread.start()
        return thread

    def requestVoteFromNode(self, k, proxy, inf, vote):
        try:
            result = proxy.requestVote(inf)
        except ConnectionRefusedError:
            print("Connection refused from Node {0} updating the vote as False".format(k))
            result = False
        except CannotSendRequest:
            print("Cannot send the message to node {0} from the node {1}".format(k, self.id))
            result = False
        except ConnectionResetError:
            print("Connection was reset in Node {0} while invoking the voteRPC by node {1}".format(k, self.id))
            result = False
        if result:
            vote.addVote()
        print("Election Result: Candidate {0} requestedNde {1} result: {2} term: {3}".format(self.id, k, result,
                                                                                             inf['term']))
        return result

    def getSimpleMajority(self):
        if self.noOfNodes % 2 == 0:
            return int(self.noOfNodes / 2 + 1)
        else:
            return int(self.noOfNodes / 2) + 1

    def getLastTerm(self):
        prevLogIndex = -1 if self.getLastIndex() == -1 else self.getLastIndex() - 1
        if prevLogIndex == -1:
            return 0
        else:
            return self.log[prevLogIndex].term

    def getLastIndex(self):
        return -1 if not self.log else len(self.log) - 1

    def createInfo(self):
        dict = {}

        dict['nodeid'] = self.id
        dict['term'] = self.currentTerm
        dict['lastindexofthelog'] = self.getLastIndex()
        dict['lastlogterm'] = self.getLastTerm()
        return dict

    def createApppendEntryInfo(self):
        dict = {}

        dict['leaderid'] = self.id
        dict['term'] = self.currentTerm
        dict['previouslogindex'] = -1 if self.getLastIndex() == -1 else self.getLastIndex() - 1
        dict['previouslogterm'] = self.getLastTerm()
        dict['values'] = None
        dict['leadercommit'] = self.commitIndex
        # print(dict)

        return dict

    def timeout(self):
        while True:
            randomTimeout = random.randint(7, 12)
            if self.state == State.FOLLOWER:
                # print("Chosen timeout for node {0} is {1}".format(self.id, randomTimeout))
                logging.debug("Chosen timeout for node {0} is {1}".format(self.id, randomTimeout))
                # sleep(randomTimeout)
                self.raftsleep(randomTimeout, self, "timeoutFlag")
                # print("Acquiring HB mutex")
                logging.debug('Acquiring HB mutex')
                self.mutexForHB.acquire()
                if self.receivedHeartBeat:
                    self.receivedHeartBeat = False
                    # print(
                    #     "Timeout occurred but Heartbeat was received by the node {0} earlier. Picking a new timeout".format(
                    #         self.id))
                    logging.debug(
                        "Timeout occurred but Heartbeat was received by the node {0} earlier. Picking a new timeout".format(
                            self.id))
                    self.mutexForHB.release()
                    # print("Releasing HB mutex")
                    logging.debug("Releasing HB mutex")
                    continue
                    # pick a new timeout
                else:
                    self.mutexForHB.release()
                    # print("Releasing HB mutex")
                    logging.debug("Releasing HB mutex")
                    # print(
                    #     "Timeout occurred NO Heartbeat was received by the node {0} earlier. Picking a new timeout".format(
                    #         self.id))
                    logging.debug(
                        "Timeout occurred NO Heartbeat was received by the node {0} earlier. Picking a new timeout".format(
                            self.id))
                    while True:
                        electionTimeout = random.randint(4, 7)
                        # we can send the timeout period to following method as well
                        # Not sure what is the best solution yet
                        result = self._invokeRequestVoteRPV()
                        if result:
                            print("Leader Elected: Node {0}".format(self.id))
                            break
                        else:
                            if self.state == State.CANDIDATE:
                                # sleep(electionTimeout)
                                self.raftsleep(electionTimeout, self, "electionTimeoutFlag")
                                print("Election timeout occurred. Restarting the election Node {0}".format(self.id))
                                logging.debug(
                                    "Election timeout occurred. Restarting the election Node {0}".format(self.id))
                            elif self.state == State.FOLLOWER:
                                print("Looks like we found a leader for the term, leader is {0}".format(self.votedFor))
                                logging.debug(
                                    "Looks like we found a leader for the term, leader is {0}".format(self.votedFor))
            elif self.state == State.LEADER:
                randomTimeout = random.randint(2, 3)
                # send heartbeats
                # self.updatecommitIndex()
                info = self.createApppendEntryInfo()
                info['value'] = None
                for k, v in self.map.items():
                    if k != self.id:
                        self.callAppendEntryForaSingleNode(k, v, hb=True)
                        # v.appendEntries(info)

                # sleep(randomTimeout)
                self.raftsleep(randomTimeout, self, "leaderTimeoutFlag")
                # This is to manually timeout the Leader
                if self.timeoutFlag:
                    self.receivedHeartBeat = False
                    self.state = State.FOLLOWER
                    continue
                # print("Sending Heartbeats")
                logging.debug("Sending Heartbeats")

    def createTimeoutThread(self):
        thread = threading.Thread(target=self.timeout)
        thread.daemon = True
        thread.start()

        thread2 = threading.Thread(target=self.updatecommitIndex)
        thread2.daemon = True
        thread2.start()
        return thread

    def updatecommitIndex(self):
        temp = self.commitIndex + 1
        while True:
            if self.state == State.LEADER:
                if self.matchIndex is None or self.nextIndex is None:
                    sleep(2)
                    # print("Leaders volatile state has not been updated retrying")
                    logging.debug("Leaders volatile state has not been updated retrying")
                    continue
                # print("Current Commit Index {0}".format(self.commitIndex))
                # print("TempCommitIndex {0}".format(temp))
                logging.debug("Current Commit Index {0}".format(self.commitIndex))
                # temp = self.commitIndex + 1
                count = 0
                if 0 <= temp < len(self.log):
                    count = count + 1
                else:
                    # print("Next index does not exist in the log next index {0}, length of log {1}".format(temp,
                    #                                                                                       len(self.log)))
                    logging.debug("Next index does not exist in the log next index {0}, length of log {1}".format(temp,
                                                                                                                  len(self.log)))
                    sleep(3)
                    continue
                for i in range(self.noOfNodes):
                    if i != self.id:
                        # print("matchedIndexInCommitThread" + str(self.matchIndex))
                        if temp <= self.matchIndex[i - 1]:
                            count = count + 1
                # print("Count: {0}".format(count))
                # print('Simple Majority: {0}'.format(self.getSimpleMajority()))
                if count >= self.getSimpleMajority():
                    if self.log[temp].term == self.currentTerm:
                        # print("Next commit index is {0}".format(temp))
                        logging.debug("Next commit index is {0}".format(temp))
                        self.commitIndex = temp
                        self.log[temp].iscommitted = True
                        temp += 1
                    else:
                        # print(
                        #     "Entry ID:{0} is replicated in majority but was not appended by current term {1} and "
                        #     "leader {1}".format(
                        #         temp, self.currentTerm, self.id))
                        logging.debug(
                            "Entry ID:{0} is replicated in majority but was not appended by current term {1} and "
                            "leader {1}".format(
                                temp, self.currentTerm, self.id))
                        temp += 1
                sleep(3)
                # print("Waked up")
                # temp += 1
            else:
                # print("Not the leader to find the commit index")
                pass

    def addRequest(self, value):
        if self.state is State.LEADER:
            # add the entry to the log
            entry = Entry(0, self.currentTerm)  # id is not the correct one so we update it in the next two lines
            entry.value = value
            entry.owner = self.id
            self.log.append(entry)
            entry.id = len(self.log) - 1
            print("Entry ID: {0}".format(entry.id))
            logging.debug("Entry ID: {0}".format(entry.id))
            _persist(self._persist, self.log)
            for k, v in self.map.items():
                if k != self.id:
                    self.callAppendEntryForaSingleNode(k, v)
            return True

        else:
            print("Node {0} is not the leader. cannot add the entry. Try the leader".format(self.id))
            logging.debug("Node {0} is not the leader. cannot add the entry. Try the leader".format(self.id))
            return False

    def callAppendEntryForaSingleNode(self, k, v, hb=False):
        # this method should spawn a thread
        try:
            while True:
                info = self.createApppendEntryInfo()
                values = []
                # print("NextIndex:" + str(self.nextIndex))
                if self.nextIndex[k - 1] > self.getLastIndex() and not hb:
                    # print("Node {0} is up to date".format(k))
                    logging.debug("Node {0} is up to date".format(k))
                    return True
                hb = False  # HB flag is set to false because we do not need to loop this indefinitely
                if not self.log:
                    # print("Log is empty, Therefore values would be empty as well. This would be a heartbeat")
                    logging.debug("Log is empty, Therefore values would be empty as well. This would be a heartbeat")
                else:
                    # print("Length " + str(len(self.log)) + " k: " + str(k) + " nextIndex " + str(
                    #     len(self.nextIndex)) + " nextIndexValue: " + str(self.nextIndex[k - 1]))
                    logging.debug("Length " + str(len(self.log)) + " k: " + str(k) + " nextIndex " + str(
                        len(self.nextIndex)) + " nextIndexValue: " + str(self.nextIndex[k - 1]))
                    if not (self.nextIndex[k - 1] > self.getLastIndex()):  # need to do this check again for HB without
                        # entries
                        values.append(self.log[self.nextIndex[k - 1]])
                        info['previouslogindex'] = int(self.nextIndex[k - 1] - 1)
                        info['previouslogterm'] = 0 if self.nextIndex[k - 1] - 1 < 0 else self.log[
                            self.nextIndex[k - 1] - 1].term
                        # print("sdf: {0}".format(info['previouslogindex']))
                        # print("sdf2: {0}".format(info['previouslogterm']))
                if len(values) == 0:
                    info['values'] = None
                else:
                    info['values'] = pickle.dumps(values)
                    # print(info['values'])
                    print(pickle.loads(info['values']))
                if self.state is State.LEADER:
                    result, term = v.appendEntries(info)
                    # print("RESULT: {0} Term {1} from Node{2}".format(result, term, k))
                    logging.debug("RESULT: {0} Term {1} from Node{2}".format(result, term, k))
                    if result:
                        # update the nextIndex and matchindex
                        # print("values: {0}".format(values))
                        if values:
                            id = values[-1].id
                            # print("buuuwwa" + str(self.matchIndex))
                            if self.matchIndex[k - 1] <= id:
                                self.matchIndex[k - 1] = id
                            self.nextIndex[k - 1] = id + 1
                        continue
                        # return True
                    else:
                        if term > self.currentTerm:
                            print("Node {0} is no longer the leader. converting to Follower".format(self.id))
                            logging.debug("Node {0} is no longer the leader. converting to Follower".format(self.id))
                            self.state = State.FOLLOWER
                            return False
                        else:
                            print("AppendEntry was rejected by node {0}".format(k))
                            print("Reducing the next index value for node {0}".format(k))
                            logging.debug("AppendEntry was rejected by node {0}".format(k))
                            logging.debug("Reducing the next index value for node {0}".format(k))
                            self.nextIndex[k - 1] = self.nextIndex[k - 1] - 1 if self.nextIndex[k - 1] > 1 else 0
                            print("try again with the last ")
                else:
                    print("Something happened. Node {0} is no longer the leader".format(self.id))
                    logging.debug("Something happened. Node {0} is no longer the leader".format(self.id))
                    return False
        except ConnectionRefusedError:
            print("Node {0} connection was refused".format(k))
            logging.error("Node {0} connection was refused".format(k))
            return False
        except CannotSendRequest:
            print("Node {0} cannot send request".format(k))
            logging.error("Node {0} cannot send request".format(k))
            return False
        except ConnectionResetError:
            print("Connection was reset in Node {0} while invoking the AppendEntries by node {1}".format(k, self.id))
            return False
        except TypeError as e:
            print(TypeError, e)
            quit(0)
        except Exception as e:
            print("Exception Occurred {0}".format(e))

    def printLog(self):
        print("Printing the log of node {0}".format(self.id))
        logging.debug("Printing the log of node {0}".format(self.id))
        for e in self.log:
            print(e)
            logging.debug(e)
        print("")
        logging.debug("")

    def _main(self):
        print('Number of arguments:', len(sys.argv), 'arguments.')
        print('Argument List:', str(sys.argv))

        recoveryMode = False
        if len(sys.argv) > 1:
            print("Recovery Mode option was input")
            recoveryInput = sys.argv[1]
            if recoveryInput == "r":
                print("Recovery Mode enabled")
                recoveryMode = True
            else:
                print("Normal Mode enabled")
                recoveryMode = False

        if len(sys.argv) > 2:
            print("Server ip is {0}".format(sys.argv[2]))
            self.HOST = sys.argv[2]
            print("Server Ip updated")

        if len(sys.argv) > 3:
            print("Client's ip is {0}".format(sys.argv[3]))
            self.clientip = sys.argv[3]

        else:
            print("User did not choose a client ip default is 127.0.0.1")
            self.clientip = "127.0.0.1"

        if len(sys.argv) > 4:
            print("user inputted client port {0}".format(sys.argv[4]))
            self.clientPort = int(sys.argv[4])
        else:
            print("User did not choose a port for the node. Random port between 55000-63000 will be selected")
            port = random.randint(55000, 63000)
            print("Random port {0} selected".format(port))
            self.clientPort = port

        if not recoveryMode:
            self._initializeTheNode()
            self._sendNodePort()
            self._createRPCServer()
            print(
                "Ready to start the Raft Server. Please wait until all the nodes are ready to continue. Then press Enter")
            if input() == "":
                print("Started Creating the Raft Server")
                self.mapofNodes = self.getMapData()
                print(self.mapofNodes)
                print("Creating the proxy Map")
                self._createProxyMap()
                print(self.map)
                logging.debug(self.map)
                self.noOfNodes = len(self.map)

                self._persist = Persist()
                self._persist.updateNetworkInfo(self.HOST, self.clientip, self.clientPort)
                self._persist.updateNodeInfo(self.id, self.mapofNodes, self.noOfNodes)
                self._persist.updateCurrentInfo(0, None, [])
                _persist(self._persist, self.log)

                # self.createThreadToListen()
                # self.createHeartBeatThread()
                self.menu(self)
        else:
            print("Skipping Initialization mode and send Node Port")
            nodeId = input("Input persistent ID: ")
            self.recover(nodeId)
            self._createRPCServer()
            self.menu(self)

    def getMapData(self):
        print("Requesting Node Map from the Server")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(3)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))
            resp2 = {}
            for k, v in resp.items():
                resp2[int(k)] = (v[0], int(v[1]))

            print(resp2)
            s.close()
            return resp2

    def _sendNodePort(self):
        # establish connection with server and give info about the client port
        print('Sending client port to Server')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(2)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            print(resp['response'])
            s.close()

    def _initializeTheNode(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Connecting to HOSTL {0} port {1}".format(self.HOST, self.SERVER_PORT))
            s.connect((self.HOST, self.SERVER_PORT))
            strReq = self.createJSONReq(1)
            jsonReq = json.dumps(strReq)

            s.sendall(str.encode(jsonReq))

            data = self.receiveWhole(s)
            resp = self.getJsonObj(data.decode("utf-8"))

            self.id = int(resp['seq'])
            print("id: " + str(self.id))
            s.close()
        currrent_dir = os.getcwd()
        finallogdir = os.path.join(currrent_dir, 'log')
        if not os.path.exists(finallogdir):
            os.mkdir(finallogdir)

        currrent_dir = os.getcwd()
        finalpersistdir = os.path.join(currrent_dir, 'persist')
        if not os.path.exists(finalpersistdir):
            os.mkdir(finalpersistdir)
        logging.basicConfig(filename="log/{0}.log".format(self.id), level=logging.DEBUG, filemode='w')

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

    def createJSONReq(self, typeReq, nodes=None, slot=None):
        # Initialize node
        if typeReq == 1:
            request = {"req": "1"}
            return request
        # Send port info
        elif typeReq == 2:
            request = {"req": "2", "seq": str(self.id), "port": str(self.clientPort)}
            return request
        # Get map data
        elif typeReq == 3:
            request = {"req": "3", "seq": str(self.id)}
            return request
        else:
            return ""

    def getJsonObj(self, input):
        jr = json.loads(input)
        return jr

    def _createRPCServer(self):
        print("Creating the RPC server for the Node {0}".format(self.id))
        print("Node {0} IP:{1} port: {2}".format(self.id, self.clientip, self.clientPort))
        thread = threading.Thread(target=self._executeRPCServer)
        thread.daemon = True
        thread.start()
        return thread

    def _executeRPCServer(self):
        server = SimpleXMLRPCServer((self.clientip, self.clientPort), logRequests=False, allow_none=True)
        server.register_instance(self)
        try:
            print("Serving........")
            server.serve_forever()
        except KeyboardInterrupt:
            print("Exiting")

    def _createProxyMap(self):
        self.map = {}
        for k, v in self.mapofNodes.items():
            print(k, v)
            uri = r"http://" + v[0] + ":" + str(v[1] + 1)
            print(uri)
            self.map[k] = ServerProxy(uri, allow_none=True)

    def printTest(self):
        print("I am node {0}".format(self.id))

    def menu(self, d):
        self.createTimeoutThread()
        while True:
            print("Display Raft DashBoard\t")
            print("Press d for Diagnostics\t")
            print("Press t for timeout manually\t")
            print("Press f for fail the process\t")
            print("Press r to print the log\t")
            print("Press e to exit the program\t")
            print("Press a to manually add an entry\t")
            print("Press any key to get the menu\t")
            resp = input("Choice: ").lower().split()
            if not resp:
                continue
            elif resp[0] == 'd':
                self._diagnostics()
            elif resp[0] == 'q':
                self.recover(self.id)
            elif resp[0] == 'r':
                self.printLog()
            elif resp[0] == 'a':
                x = input("What do you want to add?")
                self.addRequest(x)
            elif resp[0] == 'f':
                exit(0)
            elif resp[0] == 'g':
                print("BLUE: {0}".format(self.blue))
                print("RED: {0}".format(self.red))
                print("Bstate {0}".format(self.bstate))
                print("baction:{0}".format(self.baction))
                print("bgotblocked:{0}".format(self.bgotblocked))
                print("blastpunchtime:{0}".format(self.blastpunchtime))
                print("Rstate:{0}".format(self.rstate))
                print("Raction:{0}".format(self.raction))
                print("rgotblocked:{0}".format(self.rgotblocked))
                print("rlastpunchtime:{0}".format(self.rlastpunchtime))
            elif resp[0] == 't':
                self.timeoutFlag = True
                self.receivedHeartBeat = False
                self.leaderTimeoutFlag = True
                self.electionTimeoutFlag = False
            elif resp[0] == 'e':
                exit(0)

    def _diagnostics(self):
        print("Printing Diagnostics")
        print("Node {0}".format(self.id))
        print("ServerIP: {0}".format(self.HOST))
        print("Raft Server IP: {0}".format(self.clientip))
        print("Raft Port: {0}".format(self.clientip))
        print("Raft Node state: {0}".format(self.state))
        print("CommitIndex: {0}".format(self.commitIndex))
        print("Current Term : {0}".format(self.currentTerm))
        print("MatchIndex: {0}".format(self.matchIndex))
        print("NextIndex: {0}".format(self.nextIndex))
        print('Simple Majority: {0}'.format(self.getSimpleMajority()))

    def updateCommittedEntries(self):
        temp = self.commitIndex
        for i in range(temp + 1):
            if i > len(self.log) - 1:
                print("Entry is not available for commit".format(i))
            else:
                self.log[i].iscommitted = True

    def getLeaderInfo(self):
        # returns leader's id or None if there is no leader at the moment
        if self.state == State.FOLLOWER:
            return self.votedFor
        elif self.state == State.CANDIDATE:
            return None
        else:
            return self.id

    def raftsleep(self, seconds, obj, flagname=""):
        startingtime = time.perf_counter()
        flag = False if flagname == "" else getattr(obj, flagname)
        while not getattr(obj, flagname):
            now = time.perf_counter()
            if now - startingtime >= seconds:
                setattr(obj, flagname, False)
                return
        print("Flag was set to True")
        logging.debug("Flag was set to True")
        setattr(obj, flagname, False)
        print("Flag {0} changed to False-> {1}".format(flagname, getattr(obj, flagname)))

    def recover(self, id):
        file_to_read = open("persist/data{0}.pickle".format(id), "rb")
        loaded_object = pickle.load(file_to_read)
        file_to_read.close()
        print(loaded_object)

        self.HOST = loaded_object.HOST
        self.clientip = loaded_object.clientip
        self.clientPort = loaded_object.clientPort
        self.mapofNodes = loaded_object.nodeMap
        self.noOfNodes = loaded_object.noOfNodes
        self.currentTerm = loaded_object.currentTerm
        self.votedFor = loaded_object.votedFor,
        self.log = loaded_object.log
        print(self.log)
        print(loaded_object.log)
        self.id = loaded_object.id

        self._createProxyMap()

        for k, v in self.map.items():
            if k != self.id:
                v.printTest()

        # recover commitIndex and lastApplied
        commitIndex = -1
        for e in self.log:
            if e.iscommitted:
                commitIndex = commitIndex + 1
        self.commitIndex = commitIndex

        # apply the persistent state to the variables

        # reset the flags
        self.timeoutFlag = False
        self.leaderTimeoutFlag = False
        self.electionTimeoutFlag = False

        self._persist = loaded_object

    def punch(self, color, action):
        # update entries
        li = [True, False]
        success = (random.choices(li, weights=(20, 80), k=1))[0]
        punchtime = time.perf_counter()
        info = {}
        info['player'] = color
        info['action'] = action
        info['time'] = punchtime
        info['success'] = success

        self.addRequest(info)

    def blcok(self, color, block):
        blocktime = time.perf_counter()
        info = {}
        info['player'] = color
        info['time'] = blocktime
        info['success'] = None
        info['action'] = block

    def registerPlayer(self):
        # register player, gives a unique ID
        if self.state is State.LEADER:
            if self.blue is not None and self.red is not None:
                return False, None
            elif self.blue is None:
                self.addRequest({"1": "b"})
                self.blue = True
                self.bstate = 0
                self.baction = 0
                return True, "b"
            elif self.red is None:
                self.addRequest({"2": "r"})
                self.red = True
                self.rstate = 0
                self.raction = 0
                return True, "r"
        else:
            print("Node {0} is not the leader. Cannot register the player".format(self.id))
            logging.debug("Node {0} is not the leader. Cannot register the player".format(self.id))
            return False, None

    def getGameState(self):
        # return the state of the players using a dict
        # ex: {'r': 1, 'b': 2}
        # Use the mutex
        self.gameMutex.acquire()
        copy = self.log.copy()
        if self.commitIndex == -1:
            self.gameMessage = "Game hasn't started"
            self.gameMutex.release()
            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
        if self.bstate == 0 and self.rstate == 0:
            self.gameMessage = "Players joined. Ready to start"
            # self.gameMutex.release()
            # return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
        if self.bstate == 5 or self.bstate == 6:
            self.gameMutex.release()
            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
        self.blastpunchtime = 0
        self.rlastpunchtime = 0
        self.bgotblocked = False
        self.rgotblocked = False
        for i in range(len(copy)):
            val = self.log[i].value
            print("val{0}".format(val))
            if "1" in val:
                self.blue = True
                self.gameMessage = "Player BLUE joined"
            if "2" in val:
                self.red = True
                self.gameMessage = self.gameMessage + " Player RED joined"
            if 'action' in val:
                print(val['action'])
            if "player" in val:
                if val['action'] == 1 or val['action'] == 2:
                    if val['player'] == 'b':
                        self.bstate = val['action']
                    elif val['player'] == 'r':
                        self.rstate = val['action']
                if val['action'] == 3 or val['action'] == 4:
                    if val['player'] == 'b':
                        punchtime = val['time']
                        print("punchtime{0}".format(punchtime))
                        print("vlastpunch{0}".format(self.blastpunchtime))
                        if val['action'] == 3:
                            if self.bgotblocked:
                                if (punchtime - self.blastpunchtime) > 3:
                                    self.bgotblocked = False
                                    # self.blastpunchtime = punchtime
                                    if self.rstate == 2:
                                        self.bgotblocked = True
                                        self.gameMessage = "Blues Left punch got blocked"
                                        self.blastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.bstate = 5
                                            self.rstate = 6
                                            self.gameMessage = "Game Over. B's punch landed. B won!"
                                            self.blastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "B's punch missed!"
                                            self.blastpunchtime = punchtime
                                            self.bstate = val['action']
                                else:
                                    if punchtime == self.blastpunchtime:
                                        continue
                                    self.gameMessage = "B cannot punch, B got blocked earlier {0}-{1}< 3".format(
                                        punchtime, self.blastpunchtime)
                                    print("Too early. Nothing to update")
                            else:  # b was not blocked before
                                if punchtime - self.blastpunchtime > 1:
                                    self.bgotblocked = False
                                    # self.blastpunchtime = punchtime
                                    if self.rstate == 2:
                                        self.bgotblocked = True
                                        self.gameMessage = "Blues left punch got blocked"
                                        self.blastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.bstate = 5
                                            self.rstate = 6
                                            self.gameMessage = "Blues punch landed. Blue win!"
                                            self.blastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "B's punch missed"
                                            self.blastpunchtime = punchtime
                                            self.bstate = val['action']
                                else:
                                    if punchtime == self.blastpunchtime:
                                        continue
                                    self.gameMessage = "B cannot punch, B has to wait at least one second to punch {0}-{1}< 1".format(
                                        punchtime, self.blastpunchtime)
                                    print("Too early. Nothing to update")
                        if val['action'] == 4:
                            if self.bgotblocked:
                                if (punchtime - self.blastpunchtime) > 3:
                                    self.bgotblocked = False
                                    # self.blastpunchtime = punchtime
                                    if self.rstate == 1:
                                        self.bgotblocked = True
                                        self.gameMessage = "B's Right punch got blocked"
                                        self.blastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.bstate = 5
                                            self.rstate = 6
                                            self.gameMessage = "B's right punch landed. B Win!"
                                            self.blastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "B's right punch missed"
                                            self.bstate = val['action']
                                            self.blastpunchtime = punchtime
                                else:
                                    self.gameMessage = "B cannot punch, B got blocked earlier"
                                    print("Too early. Nothing to update")
                            else:  # b was not blocked before
                                if punchtime - self.blastpunchtime > 1:
                                    self.bgotblocked = False
                                    # self.blastpunchtime = punchtime
                                    if self.rstate == 1:
                                        self.bgotblocked = True
                                        self.gameMessage = "B's right punch got blocked"
                                        self.blastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.bstate = 5
                                            self.rstate = 6
                                            self.gameMessage = "B's right punch landed. B won!"
                                            self.blastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "B's right punch missed"
                                            self.bstate = val['action']
                                            self.blastpunchtime = punchtime
                                else:
                                    self.gameMessage = "B cannot punch, B has to wait at least one second after previous punch"
                                    print("Too early. Nothing to update")

                    if val['player'] == 'r':
                        punchtime = val['time']
                        if val['action'] == 3:
                            if self.rgotblocked:
                                if (punchtime - self.rlastpunchtime) > 3:
                                    self.rgotblocked = False
                                    # self.rlastpunchtime = punchtime
                                    if self.bstate == 2:
                                        self.rgotblocked = True
                                        self.gameMessage = "R's left punch got blocked"
                                        self.rlastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.rstate = 5
                                            self.bstate = 6
                                            self.gameMessage = "R's left punch landed. R won!"
                                            self.rlastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "R's left punch missed"
                                            self.rstate = val['action']
                                            self.rlastpunchtime = punchtime
                                else:
                                    self.gameMessage = "R cannot punch, R got blocked earlier"
                                    print("Too early. Nothing to update")
                            else:  # b was not blocked before
                                if punchtime - self.rlastpunchtime > 1:
                                    self.rgotblocked = False
                                    # self.rlastpunchtime = punchtime
                                    if self.bstate == 2:
                                        self.rgotblocked = True
                                        self.gameMessage = "R's left punch got blocked"
                                        self.rlastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.rstate = 5
                                            self.bstate = 6
                                            self.gameMessage = "R's left punch landed. R won!"
                                            self.rlastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "R's left punch missed"
                                            self.rstate = val['action']
                                            self.rlastpunchtime = punchtime
                                else:
                                    self.gameMessage = "R's cannot punch, R' has to wait at least one second"
                                    print("Too early. Nothing to update")
                        if val['action'] == 4:
                            if self.rgotblocked:
                                if (punchtime - self.rlastpunchtime) > 3:
                                    self.rgotblocked = False
                                    # self.rlastpunchtime = punchtime
                                    if self.bstate == 1:
                                        self.gameMessage = "R's right punch got blocked!"
                                        self.rgotblocked = True
                                        self.rlastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.rstate = 5
                                            self.bstate = 6
                                            self.gameMessage = "R's right punch landed. R won!"
                                            self.rlastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "R's right punch missed"
                                            self.rstate = val['action']
                                            self.rlastpunchtime = punchtime
                                else:
                                    self.gameMessage = "R cannot punch, R got blocked earlier"
                                    print("Too early. Nothing to update")
                            else:  # b was not blocked before
                                if punchtime - self.rlastpunchtime > 1:
                                    self.rgotblocked = False
                                    # self.rlastpunchtime = punchtime
                                    if self.bstate == 1:
                                        self.rgotblocked = True
                                        self.gameMessage = "R's right punch got blocked"
                                        self.rlastpunchtime = punchtime
                                    else:
                                        if val['success']:
                                            self.rstate = 5
                                            self.bstate = 6
                                            self.gameMessage = "R's right punch landed!. R won!"
                                            self.rlastpunchtime = punchtime
                                            self.gameMutex.release()
                                            return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}
                                        else:
                                            self.gameMessage = "R's right punch missed!"
                                            self.rstate = val['action']
                                            self.rlastpunchtime = punchtime
                                else:
                                    self.gameMessage = "R cannot punch now. wait one second!"
                                    print("Too early. Nothing to update")
        self.gameMutex.release()
        return {"b": self.bstate, "r": self.rstate, "m": self.gameMessage}

    def getNextMiner(self, blocknumber):

        leaderid = self.getLeaderInfo()
        print("Leader id {0}".format(leaderid))
        if leaderid is None:
            return None
        elif leaderid == self.id:
            return self.choosethenextminer(blocknumber)
        else:
            return self.map[leaderid].choosethenextminer(blocknumber)

    def updateblockIdvsMiner(self):
        self.blockIdvsMiner = {}
        if self.commitIndex == -1:
            return
        else:
            for i in range(len(self.log)):
                blockvsidtuple = self.log[i].value
                # print(blockvsidtuple)
                self.blockIdvsMiner[blockvsidtuple[0]] = blockvsidtuple[1]

    def choosethenextminer(self, blocknumber):
        self.updateblockIdvsMiner()
        if blocknumber in self.blockIdvsMiner:
            return self.blockIdvsMiner[blocknumber], blocknumber
        nextblockid = self.bc.getLastBlockNumber() + 1
        # find the lowest ownership node and give them the chance
        self.bc.updateTheownershipMap()
        minval = min(self.bc.ownershipMap.values())
        res = list(filter(lambda x: self.bc.ownershipMap[x] == minval, self.bc.ownershipMap))
        if 0 in res:
            res.remove(0)
        # assuming res is not empty as self.bc.ownershipMap is not empty
        min_key = random.choice(res)
        # min_key = min(self.bc.ownershipMap, key=self.bc.ownershipMap.get)
        self.blockIdvsMiner[nextblockid] = min_key
        self.addRequest((nextblockid, min_key))
        return min_key, nextblockid


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Entry:
    def __init__(self, id, term):
        self.value = None
        self.id = id
        self.term = term
        self.iscommitted = False
        self.owner = None

    def __str__(self):
        return "id:{0} term:{1} val:{2} isCommitted: {3} AddedBy: {4}\t".format(self.id, self.term, self.value,
                                                                                self.iscommitted, self.owner)


class Vote:
    def __init__(self):
        self.votes = 0
        self.mutex = threading.Lock()

    def getVotes(self):
        return self.votes

    def addVote(self):
        self.mutex.acquire()
        self.votes = self.votes + 1
        self.mutex.release()


class Persist:
    def __init__(self):
        self.SERVER_PORT = 65431
        self.nodeMap = None
        self.noOfNodes = None
        self.currentTerm = None
        self.votedFor = None
        self.log = None
        self.id = 0

    def updateNetworkInfo(self, host, clientip, clientport):
        self.HOST = host
        self.clientip = clientip  # needs to be persisted
        self.clientPort = clientport  # needs to be persisted

    def updateNodeInfo(self, id, nodeMap, noofnodes):
        self.id = id
        self.nodeMap = nodeMap  # Map about the other nodes
        self.noOfNodes = noofnodes

    def updateCurrentInfo(self, term, votedfor, log):
        self.currentTerm = term
        self.votedFor = votedfor
        self.log = log

    def __str__(self):
        return "HOST: {0} NodeID:{8} clientip:{1}  clientPort:{2}  nodeMap:{3}  noOfNodes:{4}  currentTerm:{5} votedFor:{6}\n log:{7}".format(
            self.HOST, self.clientip, self.clientPort, self.nodeMap, self.noOfNodes, self.currentTerm, self.votedFor,
            self.log, self.id)


def _persist(obj, log):
    try:
        file = open("persist/data{0}.pickle".format(obj.id), "wb")
        pickle.dump(obj, file)
        file.close()
    except Exception as e:
        print("Exception Occurred while accessing persistent storage {0}".format(e))

    if log and log is not None:
        try:
            # print("Logging tto readable storagre {0}".format(obj, id))
            copy = log.copy()
            file = open("log/log-readable{0}.txt".format(obj.id), "w")
            for e in copy:
                file.write(str(e) + "\n")
            file.close()
        except Exception as e:
            print("Exception occurred while persisting readable log {0}".format(e))


if __name__ == "__main__":
    raft = RaftS(1)
    raft._main()
