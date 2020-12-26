import os
from node import Node
from peerList import PeerList
import logging
import json
import sys
import os
import time
import shutil
import threading
from pathlib import Path


i = int(sys.argv[1])
with open("config.json") as json_data_file:
    config = json.load(json_data_file)
logName = 'deployment-'+str(i)+'.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filemode='w', filename=Path(
                        config['logLocation']) / logName)

# clearing the peer list
PeerList.getInstance().clearNodes()

# i = int(sys.argv[1])
print("creating hosting folders for nodes")
for a in range(1, i+1):
    try:
        shutil.rmtree(Path(config['hostedFolder']) / str(a))
    except:
        print("no folder")
    os.mkdir(Path(config['hostedFolder']) / str(a))
    filename = str(a)+'.pdf'
    shutil.copy(Path(config['hostedFolder']) / '4.pdf',
                    Path(config['hostedFolder']) / str(a) / filename)
print("creation done")
os.system("cd ..")



print("file download test " + str(i) + " node start")
j = 1
while(i >= j):
    file = ''
    if(j-1 == 0):
        file = str(i)+'.pdf'
    else:
        file = str(j-1)+'.pdf'
    print("node number "+str(j)+" downloading file "+file)
    threading.Thread(target=Node, args=(j, file,)).start()
    j = j+1
    if j%5 !=0:
        time.sleep(0.5)
print("file download test " + str(i) + " node end")
