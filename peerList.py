import os
import hashlib
import json
from pathlib import Path
import logging
import time

#singleton class to group the nodes
class PeerList:
    __instance = None

    @staticmethod
    def getInstance():
        if PeerList.__instance == None:
            PeerList()
        return PeerList.__instance

    def __init__(self):
        self.nodeList = []
        if PeerList.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            PeerList.__instance = self
        with open("config.json") as json_data_file:
            self.config = json.load(json_data_file)
        logName = 'peerList.log'
        logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M', filename=Path(
                                    self.config['logLocation']) / logName, filemode='w', level=logging.INFO)
        self.logger = logging.getLogger("PeerList")

    def getNodes(self):
        return self.nodeList

    def clearNodes(self):
        self.nodeList = []

    def setNodes(self, node):
        notFound = True
        for e in self.nodeList:
            if e['port'] == node['port']:
                self.nodeList.remove(e)
                self.nodeList.append(node)
                notFound = False
        if notFound:
            self.nodeList.append(node)
        self.logger.info(self.nodeList)
