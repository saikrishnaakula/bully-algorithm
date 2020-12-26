# Python application to host the files in a folder
The program uses python 3 and socket module to create the server, client programs connect to the servers specific port and gets the list of download files frpom the hosted folder and then client can choose files to download. md5 checksum is used to check the integrity of the file downloaded

## Normal Usage
- Navigate to code folder and open a command prompt 
- Run the command "python server.py"
- Run the command "python testClient.py"
- Provide the inputs and file will be downloaded

## Testing
- Navigate to code folder and open a command prompt 
- Run the server file using the command "python server.py" file, desired port should be entered in the config.json.
- Now, we can test the server by running the deployment script using the command "python deployment.py 8", the 8 at the indicates number of clients to deployee can be changed according to the need. Deployment script have multiple test cases which will test the ability of the server to handle multiple clients
- All the downloaded files will be stored in the recived folder(configurable) with naming convension [socketId]-[fileName]
- Logs will be available int the logs folder named server.log and deployment.log

## Requirements
- Python 3.7+
- Socket Module (standard library)
- Threading Module (standard library)
- OS Module (standard library)
