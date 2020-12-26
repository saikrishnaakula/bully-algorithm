import socket
import threading
import os
import hashlib
import json
from pathlib import Path
import logging
from peerList import PeerList
import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


class Handler(PatternMatchingEventHandler):

    def __init__(self, childS, dataFolder):
        self.dataFolder = dataFolder
        self.s = childS
        PatternMatchingEventHandler.__init__(self, patterns=['*.*'],
                                             ignore_directories=True, case_sensitive=False)

    def on_any_event(self, event):
        if event.is_directory:
            return None
        else:
            self.s.sendall('updateNode'.encode())
            if self.s.recv(1024).decode() == 'file list':
                files = os.listdir(self.dataFolder)
                files = str(files)
                files = files.encode()
                self.s.sendall(files)
            logging.info(self.s.recv(1024).decode())
            logging.info("File updated %s." % event.src_path)


class Node:

    def __init__(self, nodeNum, fileName):
        with open("config.json") as json_data_file:
            self.config = json.load(json_data_file)
        self.dataFolder = Path(self.config['hostedFolder']) / str(nodeNum)
        logName = 'node-'+str(nodeNum) + '.log'
        logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M', filename=Path(
                                self.config['logLocation']) / logName, filemode='w', level=logging.INFO)
        self.logger1 = logging.getLogger('node-'+str(nodeNum))
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.file_name = fileName
        self.rank = nodeNum
        self.nodeList = []
        self.downloadedTheFile = False
        self.acceptConnections()

    def acceptConnections(self):
        ip = socket.gethostbyname(socket.gethostname())
        self.s.bind((ip, 0))
        self.s.listen(50)
        self.port = self.s.getsockname()[1]
        self.logger1.info('Running on IP: '+ip)
        self.logger1.info('Running on port: '+str(self.port))
        threading.Thread(target=self.election).start()
        while 1:
            c, addr = self.s.accept()
            self.logger1.info('Client registered '+str(addr))
            threading.Thread(target=self.handleClient,
                             args=(c, addr,)).start()

    def handleClient(self, c, addr):
        try:
            while True:
                data = c.recv(1024).decode()
                if(data == 'registerNode'):
                    c.send('port'.encode())
                    port = int(c.recv(1024).decode())
                    self.logger1.info('Registering the node '+str(port))
                    c.send('file list'.encode())
                    files = eval(c.recv(1024).decode())
                    self.nodeList.append(
                        {'port': port, 'childPort': addr[1], 'files': files, 'active': True})
                    self.logger1.info(self.nodeList)
                    self.logger1.info('Registered the node '+str(port))
                    c.sendall('Registered successfully'.encode())
                if(data == 'updateNode'):
                    c.send('file list'.encode())
                    files = eval(c.recv(1024).decode())
                    for n in self.nodeList:
                        if n['childPort'] == addr[1]:
                            n['files'] = files
                            self.logger1.info(
                                'Updated the file list of the node '+str(n['port']))
                    self.logger1.info(self.nodeList)
                    c.sendall('Updated successfully'.encode())
                if(data == 'listofnode'):
                    resp = []
                    c.send('fileName'.encode())
                    fileName = c.recv(1024).decode()
                    for n in self.nodeList:
                        if n['active'] == True and n['childPort'] != addr[1]:
                            for l in n['files']:
                                if fileName == l:
                                    resp.append({"port": n['port']})
                        else:
                            self.logger1.info(
                                'Sent list of files to '+str(n['port']))
                    resp = str(resp)
                    resp = resp.encode()
                    c.sendall(resp)
                if(data == 'electyourself'):
                    self.logger1.info('Election called by '+str(addr))
                    self.electLeader()
                if(data == 'newleader'):
                    # self.observer.stop()
                    # self.dhtConn.shutdown(2)
                    # self.dhtConn.close()
                    c.sendall("port".encode())
                    if self.leader_port == self.port:
                        self.logger1.info('Leader changed '+str(self.port))
                    self.leader_port = int(c.recv(1024).decode())
                    self.logger1.info(
                        'Connecting to the new leader '+str(self.leader_port))
                    self.connectToLeader()
                if(data == 'md5'):
                    c.sendall("fileName".encode())
                    c.sendall(self.md5(self.dataFolder /
                                       c.recv(1024).decode()).encode())
                    self.logger1.info('MD5 token sent to '+str(addr))
                if(data == 'downloadfile'):
                    c.sendall("fileName".encode())
                    data = c.recv(1024).decode()
                    filePath = self.dataFolder / data
                    fileSize = str(os.path.getsize(filePath))
                    self.logger1.info('Download requested for file ' +
                                      data + ' size ' + fileSize
                                      + 'Bytes by '+str(addr))
                    if not os.path.exists(filePath):
                        self.logger1.info(
                            'File doesnt exit '+data + ' to '+str(addr))
                        c.sendall("file-doesn't-exist".encode())
                    else:
                        c.sendall(fileSize.encode())
                        self.logger1.info(
                            'Sending file '+data + ' to '+str(addr))
                        if data != '':
                            tic = time.perf_counter()
                            file = open(filePath, 'rb')
                            fdata = file.read(1024)
                            while fdata:
                                c.sendall(fdata)
                                fdata = file.read(1024)
                            file.close()
                            toc = time.perf_counter()
                            totalTime = str(f"{toc - tic:0.4f} seconds")
                            self.logger1.info(
                                'File '+data + ' sent in ' + totalTime + ' to '+str(addr))
                            self.logger1.info(totalTime)
                            c.shutdown(socket.SHUT_RDWR)
                            c.close()
        except:
            self.logger1.info('Connection closed for '+str(addr[1]))

    def election(self):
        NodeListFile = PeerList.getInstance().getNodes()
        listLen = len(NodeListFile)
        if  listLen == 0:
            PeerList.getInstance().setNodes({
                "port": self.port,
                "leader": True,
                "rank": self.rank,
                "active": True
            })
            self.logger1.info("Leader elected "+str(self.port))
            self.leader_port = self.port
            self.connectToLeader()
        elif listLen % 5 == 0:
            # some randome logic to simulate reelction
            for e in NodeListFile:
                if e['leader']:
                    e['leader']= False
                    PeerList.getInstance().setNodes(e)
            PeerList.getInstance().setNodes({
                "port": self.port,
                "leader": False,
                "rank": self.rank,
                "active": True
            })
            self.logger1.info("Reelection "+str(self.port))
            self.electLeader()
        else:
            PeerList.getInstance().setNodes({
                "port": self.port,
                "leader": False,
                "rank": self.rank,
                "active": True
            })
            for e in NodeListFile:
                if e['leader'] and e["active"]:
                    try:
                        self.leader_port = e['port']
                        self.logger1.info(
                            'Connecting to the new leader '+str(self.leader_port))
                        self.connectToLeader()
                    except Exception as e:
                        print(e)
                        self.electLeader()

    def electLeader(self):
        NodeListFile = PeerList.getInstance().getNodes()
        noHigerRank = True
        for e in NodeListFile:
            if e['active'] and e['rank'] > self.rank:
                try:
                    childS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    childS.connect((socket.gethostbyname(
                        socket.gethostname()), int(e['port'])))
                    childS.sendall('electyourself'.encode())
                    childS.shutdown(2)
                    childS.close()
                    noHigerRank = False
                    break
                except:
                    continue
        if noHigerRank:
            for e in NodeListFile:
                if e['port'] == self.port:
                    e['leader'] = True
                    PeerList.getInstance().setNodes(e)
                    self.leader_port = e['port']
                    self.logger1.info("Leader elected "+str(e['port']))
                    self.notifyNodes()
                    self.connectToLeader()
                    break

    def notifyNodes(self):
        NodeListFile = PeerList.getInstance().getNodes()
        for e in NodeListFile:
            if e['active']:
                try:
                    childS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    childS.connect((socket.gethostbyname(
                        socket.gethostname()), int(e['port'])))
                    childS.sendall('newleader'.encode())
                    if childS.recv(1024).decode() == 'port':
                        childS.sendall(str(self.port).encode())
                    childS.shutdown(2)
                    childS.close()
                except:
                    e['active'] = False
                    PeerList.getInstance().setNodes(e)

    def connectToLeader(self):
        self.observer = Observer()
        self.target_ip = socket.gethostbyname(socket.gethostname())
        self.dhtConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dhtConn.connect((self.target_ip, int(self.leader_port)))
        self.dhtConn.sendall('registerNode'.encode())
        if self.dhtConn.recv(1024).decode() == 'port':
            self.dhtConn.sendall(str(self.port).encode())
        if self.dhtConn.recv(1024).decode() == 'file list':
            files = os.listdir(self.dataFolder)
            files = str(files)
            files = files.encode()
            self.dhtConn.sendall(files)
        self.logger1.info(
            'Node registered with DHT Server '+str(self.leader_port))
        self.logger1.info(self.dhtConn.recv(1024).decode())
        event_handler = Handler(self.dhtConn, self.dataFolder)
        self.observer.schedule(event_handler, self.dataFolder, recursive=True)
        self.observer.start()
        if not self.downloadedTheFile:
            self.downloadCall()

    def downloadCall(self):
        # yn = input("may i download(y/n) -->")
        # if(yn=='y'):
        self.dhtConn.sendall('listofnode'.encode())
        if self.dhtConn.recv(1024).decode() == "fileName":
            self.dhtConn.sendall(str(self.file_name).encode())
        data = self.dhtConn.recv(4098).decode()
        self.downloadedTheFile = True
        for d in eval(data):
            self.downloadFileSingle(d['port'], self.file_name)
            break

    def downloadFileSingle(self, port, file_name):
        DownloadS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        DownloadS.connect((self.target_ip, int(port)))
        DownloadS.sendall('md5'.encode())
        if DownloadS.recv(1024).decode() == 'fileName':
            DownloadS.sendall(file_name.encode())
        hash = DownloadS.recv(1024).decode()
        DownloadS.sendall('downloadfile'.encode())
        if DownloadS.recv(1024).decode() == 'fileName':
            DownloadS.sendall(file_name.encode())
        size = DownloadS.recv(1024).decode()
        if size == "file-doesn't-exist":
            self.logger1.info("File doesn't exist on server.")
            DownloadS.shutdown(socket.SHUT_RDWR)
            DownloadS.close()
        else:
            total = 0
            size = int(size)
            name = self.dataFolder / file_name
            with open(name, 'wb') as file:
                while 1:
                    data = DownloadS.recv(1024)
                    total = total + len(data)
                    file.write(data)
                    if total >= size:
                        break
                file.close()
            if hash == self.md5(name):
                self.logger1.info(file_name+' successfully downloaded.')
                print(file_name, 'successfully downloaded.')
            else:
                self.logger1.info(file_name+' unsuccessfully downloaded.')
                print(file_name, 'unsuccessfully downloaded.')
        DownloadS.shutdown(socket.SHUT_RDWR)
        DownloadS.close()

    def md5(self, name):
        hash_md5 = hashlib.md5()
        with open(name, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    # ping test every 2 mins
    def ping_test_clients(self):
        while True:
            for n in self.nodeList:
                try:
                    childS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    childS.connect((socket.gethostbyname(
                        socket.gethostname()), int(n['port'])))
                    childS.shutdown(2)
                    childS.close()
                except:
                    n['active'] = False
            time.sleep(120)
            self.logger1.info(self.nodeList)
            # time.sleep(120.0 - ((time.time() - starttime) % 60.0))

# node = Node(9, '1.pdf')
