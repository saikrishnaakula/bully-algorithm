from node import Node
import sys


nodeNum = int(sys.argv[1])
if(nodeNum-1 == 0):
    file = '1.pdf'
else:
    file = str(nodeNum-1)+'.pdf'
client = Node(nodeNum,file)
