import json
import socket
import sys
import threading
import time


class Server():

    def __init__(self, maxNodes=4, host="127.0.0.1"):
        self.HOST = host  # Standard loopback interface address (localhost)
        self.PORT = 65431  # Port to listen on (non-privileged ports are > 1023)
        self.mutex = threading.Lock()
        self.seq = 0
        self.map = {}
        self.count = maxNodes

    def getNewSeq(self):
        self.mutex.acquire()
        try:
            self.seq = self.seq + 1
            temp = self.seq
        finally:
            self.mutex.release()
        return temp

    def receiveWhole(self, conn):
        data = conn.recv(1024)
        return data

    def getJsonObj(self, bytestr):
        jr = json.loads(bytestr.decode("utf-8"))
        return jr

    def sendNewMap(self):
        thread = threading.Thread(target=self.sendMap)
        thread.daemon = True
        thread.start()

    def sendMap(self, ):
        for key, value in self.map.items():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, int(value)))
                y = json.dumps(self.map)
                s.sendall(str.encode(y))
                s.close()

    def main(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Server IP for binding is {0} Server port for binding is {1}".format(self.HOST, self.PORT))
            s.bind((self.HOST, self.PORT))
            while (True):
                s.listen()
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    # time.sleep(2)
                    while True:
                        data = self.receiveWhole(conn)
                        if data == b'':
                            break
                        jsonreq = self.getJsonObj(data)
                        reqType = jsonreq["req"]
                        if reqType == "1":
                            temp = self.getNewSeq()
                            x = {"seq": str(temp)}
                            y = json.dumps(x)
                            print(y)
                            conn.sendall(str.encode(y))
                        if reqType == "2":
                            self.map[jsonreq['seq']] = (addr[0], jsonreq['port'], jsonreq['n'], jsonreq['e'])

                            # create response
                            x = {"response": "success"}
                            y = json.dumps(x)
                            print(self.map)
                            conn.sendall(str.encode(y))
                        if reqType == "3":
                            # print("Request from {0} for node data".format(jsonreq['seq']))
                            y = json.dumps(self.map)
                            conn.sendall(str.encode(y))
                            print("New node {0} added".format(jsonreq['seq']))
                            self.count = self.count - 1
                            print("Nodes left to be informed: {0}".format(str(self.count)))
                            if self.count == 0:
                                print("Maximum Number of nodes connected. Server is closing down")
                                s.close()
                                exit(0)
                            # self.sendNewMap()


if __name__ == '__main__':
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    mxnodes = 4
    if len(sys.argv) > 1:
        print("Maximum Number of nodes allowed {0}".format(sys.argv[1]))
        mxnodes = int(sys.argv[1])
    else:
        print("Maximum number of nodes was not inputted. Default value is 4")

    defaultServerhost = "127.0.0.1"
    if len(sys.argv) > 2:
        print("Server IP is inputted {0}".format(sys.argv[2]))
        defaultServerhost = sys.argv[2]
    else:
        print("Server IP was not given -> default to localhost 127.0.0.1")
    serv = Server(mxnodes, defaultServerhost)
    serv.main()
