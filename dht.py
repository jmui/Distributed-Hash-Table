
#Assignment 4
#COMP 3010
#Justin Mui
#7624249


import socket
import select
import os
import sys
import random
import json
import copy
import datetime
import pprint


#create json object for a message
def buildMessage(HOST, PORT, ID, cmd):
	msg = {}
	msg['cmd'] = cmd
	msg['port'] = PORT
	msg['hostname'] = HOST
	msg['ID'] = ID
	jsonMsg = json.dumps(msg)

	return jsonMsg


#create json object for find message
def buildFindMsg(HOST, PORT, ID, queryID):
	msg = {}
	msg['cmd'] = 'find'
	msg['port'] = PORT
	msg['hostname'] = HOST
	msg['ID'] = ID
	msg['hops'] = 0
	msg['query'] = queryID
	jsonMsg = json.dumps(msg)
	return jsonMsg

#increment hop count for received 'find' message
def incrementFind(jsonData):
	hopCount = jsonData['hops']

	if not isinstance(hopCount, int):
		hopCount = int(hopCount)

	hopCount +=1
	jsonData['hops'] = hopCount

	return jsonData

#create owner message to send
def buildOwnerMsg(jsonData):
	msg = {}
	msg['cmd'] = 'owner'
	msg['hostname'] = jsonData['hostname']
	queryPort = jsonData['port']
	queryID = jsonData['ID']
	queryHops = jsonData['hops']

	#ensure these are stored as ints
	if not isinstance(queryPort, int):
		queryPort = int(queryPort)
	if not isinstance(queryID, int):
		queryID = int(queryID)
	if not isinstance(queryHops, int):
		queryHops = int(queryHops)

	queryHops += 1

	msg['port'] = queryPort
	msg['ID'] = queryID
	msg['hops'] = queryHops

	jsonMsg = json.dumps(msg)
	return jsonMsg



def buildPredMsg(HOST, PORT, ID, predHOST, predPORT, predID):
	me = {'{"hostname": "' + str(HOST) + '", "port": ' + str(PORT) + ', "ID": ' + str(myKey) + '}'}
	thePred = {'{"hostname": "' + str(predHOST) + '", "port": ' + str(predPORT) + ', "ID": ' + str(predID) + '}'}

	msg = {}
	msg['me'] = me
	msg['cmd'] = 'myPred'
	msg['thePred'] = thePred

	jsonMsg = json.dumps(msg)
	return jsonMsg



gotFirstNode = False
foundPred = False
theEnd = False

currCmd = ''

#generate key
maxKey = 2**16 - 2
myKey = random.randint(1, maxKey)
#myKey = 65532
print('My key: ' + str(myKey) + '\n')

logFile = open('log.txt', 'w')	#create log file

#client hostname and port
HOST = socket.getfqdn()
PORT = 15004

#bootstrap hostname, port, ID
bootData = ''
bootHOST = 'silicon.cs.umanitoba.ca'
bootPORT = 15000
bootID = 2**16 - 1

#first node hostname, port, ID
firstHOST = ''
firstPORT = 0
firstID = 0

#predecessor hostname, port, ID
predHOST = ''
predPORT = 0
predID = 0

#next node for traversing ring
nextData = ''
nextHOST = ''
nextPORT = 0
nextID = 0

#previous node
prevHOST = 'silicon.cs.umanitoba.ca'
prevPORT = 15000
prevID = 2**16 - 1

#tuples for host,port
address = (HOST, PORT)
bootAddress = (bootHOST, bootPORT)


#create socket
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
clientSocket.bind(address)
clientSocket.settimeout(2)




#json message for joining ring
joinMsg = buildMessage(HOST, PORT, myKey, 'pred?')
joinMsg = str(joinMsg)

#send message to bootstrap to join
clientSocket.sendto(joinMsg, bootAddress)

while not foundPred:

	#got first node from bootstrap
	if not gotFirstNode:
		try:
			recvData, addr = clientSocket.recvfrom(2048)
			print(recvData)
			jsonData = json.loads(recvData)
			gotFirstNode = True
			bootData = jsonData['me']	#bootstrap info
			currCmd = jsonData['cmd']
			nextData = jsonData['thePred']
			nextHOST = nextData['hostname']
			nextPORT = nextData['port']
			nextID = nextData['ID']

			firstData = nextData
			firstHOST = nextHOST
			firstPORT = nextPORT
			firstID = nextID
			#convert to int if it's not an int
			if not isinstance(nextPORT, int):
				nextPORT = int(nextPORT)
			if not isinstance(nextID, int):
				nextID = int(nextID)
			#pprint.pprint(nextData)

			if nextID < myKey and nextID != 0:
				#set predecessor 
				predHOST = nextHOST
				predPORT = nextPORT
				predID = nextID

				print('*** My predecessor ***')
				print('ID: ' + str(predID))
				print('Host: ' + str(predHOST))
				print('Port: ' + str(predPORT))

				#send setPred to predecessor
				setPredMsg = buildMessage(HOST, PORT, myKey, 'setPred')
				prevAddress = (prevHOST, prevPORT)
				clientSocket.sendto(setPredMsg, prevAddress)
				foundPred = True
		except:
			print('No reply, joining from last predecessor')
			gotFirstNode = True
			predID = prevID
			predPORT = prevPORT
			predHOST = prevHOST




	#find location to join ring
	if gotFirstNode and not foundPred:

		try:
			nextAddress = (nextHOST, nextPORT)
			predMsg = buildMessage(HOST, PORT, myKey, 'pred?')
			clientSocket.sendto(predMsg, nextAddress)
			recvData, addr = clientSocket.recvfrom(2048)
			jsonData = json.loads(recvData)
			print(recvData)
			prevID = nextID
			prevPORT = nextPORT
			prevHOST = nextHOST

			currCmd = jsonData['cmd']
			nextData = jsonData['thePred']
			nextHOST = nextData['hostname']
			nextPORT = nextData['port']
			nextID = nextData['ID']

			#convert to int if it's not an int
			if not isinstance(nextPORT, int):
				nextPORT = int(nextPORT)
			if not isinstance(nextID, int):
				nextID = int(nextID)
			#print(type(nextID))

			if nextID < myKey and nextID != 0: #and nextID > 1:
				#set predecessor 
				predHOST = nextHOST
				predPORT = nextPORT
				predID = nextID

				print('My predecessor:')
				print('ID: ' + str(predID))
				print('Host: ' + str(predHOST))
				print('Port: ' + str(predPORT))

				#send setPred to predecessor
				setPredMsg = buildMessage(HOST, PORT, myKey, 'setPred')
				prevAddress = (prevHOST, prevPORT)
				clientSocket.sendto(setPredMsg, prevAddress)
				foundPred = True

			elif nextID == firstID or nextID == 0:	#end of the ring
				#set predecessor as first node
				predHOST = firstHOST
				predPORT = firstPORT
				predID = firstID

				print('ID: ' + str(predID))
				print('Host: ' + str(predHOST))
				print('Port: ' + str(predPORT))
				setPredMsg = buildMessage(HOST, PORT, myKey, 'setPred')
				prevAddress = (prevHOST, prevPORT)
				clientSocket.sendto(setPredMsg, prevAddress)
				foundPred = True
		except:
			logFile.write(str(datetime.datetime.now()) + ' | Timeout, will continue searching for predecessor\n')
			#reset successor to previous successor if current successor isn't responsing
			nextHOST = prevHOST
			nextPORT = prevPORT
			nextID = prevID





socketFD = clientSocket.fileno()

while not theEnd:

	(readFD, writeFD, errorFD) = select.select([socketFD, sys.stdin],[],[], 2)

	#wait for standard input or socket
	for desc in readFD:
		if desc == socketFD:  #socket
			try:
				recvData, addr = clientSocket.recvfrom(2048)
				#sys.stdout.write(recvData)
				jsonData = json.loads(recvData)
				currCmd = jsonData['cmd']

				if currCmd == 'setPred':	#received setPred message
					print(currCmd + str(addr) + '\n')
					predHOST = jsonData['hostname']
					predPORT = jsonData['port']
					predID = jsonData['ID']

				elif currCmd.lower() == 'pred?':	#received pred? message
					print(currCmd + str(addr))
					myPredMsg = buildMyPred(HOST, PORT, myKey, predHOST, predPORT, predID)
					clientSocket.sendto(myPredMsg, addr)				

				elif currCmd.lower() == 'find':	#received find message
					print(currCmd + str(addr) + '\n')
					recvKey = jsonData['ID']
					if not isinstance(recvKey, int):
						recvKey = int(recvKey)	

					if recvKey == predID:	#send owner message
						ownerMsg = buildOwnerMsg(jsonData)
						ownerMsg = str(ownerMsg)
						predAddr = (predHOST, predPORT)
						clientSocket.sendto(ownerMsg, predAddr)
					else:	#send find message
						updatedFind = incrementFind(jsonData)
						updatedFind = str(updatedFind)
						predAddr = (predHOST, predPORT)
						clientSocket.sendto(predFind, predAddr)

				elif currCmd.lower() == 'owner':	#received owner message, log to file
					print(currCmd + str(addr) + '\n')
					logFile.write(str(datetime.datetime.now()) + ' | ' + 'Queried ID: ' + str(jsonData['ID']) + ', Hops: ' + str(jsonData['hops']) + '\n')

			except:
				#log timeout
				logFile.write(str(datetime.datetime.now()) + ' | Timeout, will wait for socket or standard input\n')





		else:	#stdin
			msgStdin= sys.stdin.readline()

			if msgStdin == '\n':	#empty string, close socket and exit
				clientSocket.close()
				print("Closing socket")
				theEnd = not theEnd
				logFile.close()	#close log file
				break

			else:	#attempt to create ID from input, then send 'find' message to predecessor

				keyToFind = msgStdin
				try:
					keyToFind = int(keyToFind)
				except:
					keyToFind = random.randint(1, maxKey)

				#generate new random key if entered key is lower than this node's key, or invalid input
				if keyToFind < myKey:
					keyToFind = random.randint(1, maxKey)
					while keyToFind < myKey:
						keyToFind = random.randint(1, maxKey)

				#print('Key to find: ' + str(keyToFind))
				findMsg = buildFindMsg(HOST, PORT, myKey, keyToFind)
				findMsg = str(findMsg)
				predAddr = (predHOST, predPORT)
				print("pred address " + str(predHOST) + " " + str(predPORT) + '\n')
				clientSocket.sendto(findMsg, predAddr)


	


