#!/usr/bin/env python2.3
#
#  SSN.py
#  
#
#  Created by Jessica Clarke on 24/06/10.
#

#regular modules
import socket, e32, sys, re, time, messaging, urllib, httplib, e32db, os
import lightblue


# print outputs to file
sys.stdout = open(u'e:\\SSN\\outputLog.txt', 'w')
sys.stderr = open(u'e:\\SSN\\SSN_150210Err.txt', 'w')

#-----------------------------------------------------------
# cycles the application continuously, with 10 second break to
# allow for getting away from sensors (sleep not implemented
# on the sensors)
#-----------------------------------------------------------
def main():
	while(1):
		trace("start")
		findDevices()
		e32.ao_sleep(10)

#-----------------------------------------------------------
# formats the timestamp
#-----------------------------------------------------------
def dateFormat(inputDate):
	return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(inputDate))+"+0000"

#-----------------------------------------------------------
# search for bluetooth evices using lightblue module
# determine if they are a sensor ('sensor' is included
# in the device name
#-----------------------------------------------------------
def findDevices():
	global sockBT, runType, deviceList
	
	# For Background device discovery: 
	# deviceA[0] = addr
	# deviceA[1] = name
	
	sockBT = lightblue.socket()
	deviceList = lightblue.finddevices()
	
	for deviceA in deviceList:
		if deviceA == None:
			e32.ao_sleep(30)
			trace("No devices found")
			findDevices()
		elif re.match('Sensor', deviceA[1]) != None:
			target = (deviceA[0], 1)
			trace("device found")
			connectDevice(target)

#-----------------------------------------------------------
# open the bluetooth socket to connect to the selected device
#-----------------------------------------------------------
def connectDevice(target):
	sockBT.connect(target)
	trace("device connected")
	initiateCommunicationWithSensor(target)

#-----------------------------------------------------------
# start communication with the selected device (requires target
# address) and send appropriate value. 
# harvests the data from the sensor in prep for sorting
#-----------------------------------------------------------	
def initiateCommunicationWithSensor(target):
	
	# send a '~' to initialise communication
	sockBT.send('~')
	
	# send a 1, required by the sensor for normal harvest
	# send a 2, random data
	# send a 3, harvest data, no deletion 
	sockBT.send('1')
			
	# receive the data and store in buffer
	bit = sockBT.recv(1)
	buffer = ''
	
	while (bit != '\n'):
		buffer += bit
		bit = sockBT.recv(1)
		data = buffer
	
	# Send ~6 to delete sensor data. Close the socket, and sort the data. 
	sockBT.send('~6')
	sockBT.close()
	trace(data)
	sortData(data, target[0])

#-----------------------------------------------------------
# sort the data into appropriate values for storage in the DB
# get ready to send to database
#-----------------------------------------------------------
def sortData(data, targetAddr):
	global sensorAddr, type, flowRate, timeStamp, solarPowerStats, batteryPowerStats, depth, totalOutput
	data = data.split()
	
	#strip the first 3 couplets of the mac adddress off as these are redundant... 
	#essentially stripping the first 9 chars
	sensorAddr = targetAddr[9:]
	
	timeStamp = time.time()
	#message = ''
	
	#data values as read from buffer
	sensorType = data[0]
	batteryPowerStats = data[1]
	solarPowerStats = data[2]
	harvestTime = (float)(data[3]) #the time at which the harvest occurs
	
	#sensorType = water meter
	if sensorType == '2':
		#counter = 1
		depth = 0
		type = 'waterusage'
		totalOutput = 0

		#convert harvest time to seconds 
		convertedHarvestTime = harvestTime/1000
		
		#set up the timestamp to ensure intervals occur at right time.
		timeStamp = timeStamp - convertedHarvestTime 
		
		# This loop collects and sorts the data and formats it for the database. 
		# once in the database, it can be sent to the SOS. 
		for x in range (4, len(data), 2):
			tics = (float)(data[x])
			interval = (float)(data[x+1])
			
			#for cumulative data 
			litres = tics *5
			totalOutput = totalOutput + litres
			
			convertedIntervalToSeconds = interval / 1000
			
			if convertedIntervalToSeconds == 0:
				flowRate = 0
			else:
				flowRate = 5/convertedIntervalToSeconds  #flowrate 1 tic in the interval... 

			totalClusterInterval = tics * convertedIntervalToSeconds
			timeStamp = timeStamp - totalClusterInterval
			formattedTime = dateFormat(timeStamp)
			
			trace(formattedTime)
			
			#flowRate = flowRate * 3600
			#print flowRate
			flowRate = round(flowRate, 3)
	
			trace("Sending to DB now...")
			message = (sensorAddr, type, flowRate, timeStamp, solarPowerStats, batteryPowerStats, depth, totalOutput)
			trace(message)
			
			sendToDB(sensorAddr, type, flowRate, timeStamp, solarPowerStats, batteryPowerStats, depth, totalOutput)
	else:
		trace("Bad sensorType value")
	
	trace("all data sorted")
	setupExtract()
	
#-----------------------------------------------------------
# sends data to database on the phone then closes database
#-----------------------------------------------------------
def sendToDB(sensorAddr, sensorType, flowRate, timeStamp, solarPowerStats, batteryPowerStats, depth, totalLitres):
	global dbpath, dbm, dbv
	dbm = e32db.Dbms()
	dbv = e32db.Db_view()
	
	path = 'e:\\SSN\\'
	dbname = 'SSNdb.db'
	dbpath = path+dbname
	
	if not os.path.exists(dbpath):
		trace("os.path not found, create db")
		dbm.create(unicode(dbpath))
		dbm.open(unicode(dbpath))
		dbm.execute(u'CREATE TABLE sensordata (sensorAddr VARCHAR, sensorType VARCHAR, flowRate VARCHAR, timeStamp INTEGER, solarPowerStats VARCHAR, batteryPowerStats VARCHAR, depth INTEGER, totalLitres INTEGER)')
		trace("database created")
	else: 
		trace("db found, opening")
		dbm.open(unicode(dbpath))
	
	dbv.prepare(dbm, u'SELECT * FROM sensordata')
	dbm.execute(u'INSERT INTO sensordata VALUES (\'%s\',\'%s\', \'%s\', %s, \'%s\', \'%s\', %d, %d)'%(sensorAddr, sensorType, flowRate, timeStamp, solarPowerStats, batteryPowerStats, depth, totalLitres))
	
	dbm.close()
	trace("DB filled and closed")

#-----------------------------------------------------------
# set up the extraction of the data from the database
# for loop - allows us to extract diff types of data
#-----------------------------------------------------------	
def setupExtract():
	trace("Commence loop to insert data to SOS")
	#loop 4 times, to enter initial data, batt data, solar data, and totalLitres
	for i in range (1, 5):
		#assign appropriate values to the column, phenom, and metric depending on the loop iteration
		if i == 1:
			trace("Water usage loop")
			column = 3 #flowRate
			phenom = 'water-usage'
			metric = 'L/m'
			extractDBData(column, phenom, metric)
	
		if i == 2:
			trace("solar power loop")
			column = 5 #solarpower
			phenom = 'water-solar'
			metric = 'percent'
			#extractDBData(column, phenom, metric)
			#not implemented because of the following error
			"""line 281: encodedPoints = encodedPoints + dateFormat(date) + ',{$meter}_'+ sensorMac+',' + str(outputPoints[date]) TypeErr: cannot concatenate 'str and 'NoneType' objects"""
						
		if i == 3:
			trace("battery power loop")
			column = 6 #battPower
			phenom = 'water-battery'
			metric = 'percent'
			#extractDBData(column, phenom, metric)
			
		if i == 4:
			trace("total flow loop")
			column = 8 #cumulativeData 
			phenom = 'water-total'
			metric = 'L'
			#extractDBData(column, phenom, metric)
		
	trace("for loop complete")
	
#-----------------------------------------------------------
# extracts the appropriate data from the database and 
# prepares it for the SOS
#-----------------------------------------------------------	
def extractDBData(column, phenom, metric):
	global dbpath, dbm, dbv, sensorAddr, type, flowRate, timeStamp, solarPowerStats, batteryPowerStats, depth, totalOutput, outputPoints
	
	depth = None
	sensorMac = None
	
	dbm.open(unicode(dbpath))
	dbv.prepare(dbm, u'SELECT * FROM sensordata ORDER BY depth, sensorAddr, timestamp')
	dbv.first_line()
	
	##Table Columns: for reference in loop
	#col1 = sensorAddr
	#col2 = sensorType
	#col3 = flowRate
	#col4 = timeStamp
	#col5 = solarPowerStats
	#col6 = batteryPowerStats
	#col7 = depth
	#col8 = totalLitres
	
	for j in range(0, dbv.count_line()-1):
		dbv.get_line()   
		if sensorMac != str(dbv.col(1)) or depth != str(dbv.col(7)):
			if sensorMac != None or depth !=None:
				formatForSOS(depth, type, sensorMac, outputPoints, phenom, metric)

			outputPoints = {dbv.col(4):str(dbv.col(column))}
			sensorMac = str(dbv.col(1))
			depth = str(dbv.col(7))
			type = str(dbv.col(2))
		else:
			outputPoints[dbv.col(4)] = str(dbv.col(column))
		dbv.next_line()	
			
	trace("data extracted")
	message = ("ouputPoints: ", outputPoints, "phenom: ", phenom)
	trace(message)
	formatForSOS(depth, type, sensorMac, outputPoints, phenom, metric)
	
	dbm.close()		
	
#-----------------------------------------------------------
# Prepares the data for the SOS
# replaces the tags in the xml file with the new data
#-----------------------------------------------------------	
def formatForSOS(depth, type, sensorMac, outputpoints, phenom, metric):
	global data, dbm
	
	body = open('e:\\SSN\\insertsensorobs.xml').read()
	
	sorteddates = outputpoints.keys()
	sorteddates.sort() 
	
	endDate = sorteddates[len(sorteddates)-1]
	pointcount = 0
	
	e32.ao_sleep(3)
	
	encodedPoints = ''
	for date in sorteddates:   
		if encodedPoints != '':
			encodedPoints = encodedPoints + '@@'
			pointcount =+ 1
		
		encodedPoints = encodedPoints + dateFormat(date) + ',{$meter}_' + sensorMac + ',' + str(outputpoints[date])
	
		
		
	# replace the time and value placeholders.	
	body = body.replace('{$startdate}', dateFormat(sorteddates[0]))
	body = body.replace('{$enddate}', dateFormat(endDate))
	body = body.replace('{$points}', encodedPoints)
	body = body.replace('{$phenom}', phenom)
	body = body.replace('{$sensorMac}', sensorMac)
	body = body.replace('{$pointcount}', str(pointcount))
	body = body.replace('{$metric}', metric)

	if type == 'EASYAG':
		body = body.replace('{$type}', type + ':' + depth)
		body = body.replace('{$meter}', 'soilMoisture')

	else:
		body = body.replace('{$type}', type)
		body = body.replace('{$meter}', 'waterMeter')
	
	dblog = open('e:\\SSN\\Log.txt', 'w')
	dblog.write(body)
	dblog.close()
	
	e32.ao_sleep(4)
	trace("sending to sos")
	sendToSOS(body, sensorMac, depth, endDate)

#-----------------------------------------------------------
# uses http to send the filled in xml file to the SOS
#-----------------------------------------------------------
def sendToSOS(body, sensorMac, depth, endDate):	
	apFile = open('e:\\SSN\\apid.txt', 'rb')
	setting = apFile.read()
	apid = eval(setting)
	apFile.close()
	
	#error checking here for the credit etc. "can you hear me?, yes? wicked!"
	trace("got credit") 
	apo = socket.access_point(apid)
	socket.set_default_access_point(apo)
	sockIP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# send the request.
	sockIP = httplib.HTTPConnection('150.229.66.73', 80)
	sockIP.request('POST', '/HutchinsSOS/sos', body)
	trace("still here")
	# check the response.	
	resp = sockIP.getresponse()
	responsedoc = resp.read()	
	dbm.execute(u'DELETE FROM sensordata WHERE sensorAddr=\'%s\' AND depth = %s AND timeStamp <= %s'%(sensorMac, depth,  endDate))
	trace("Transmission to SOS complete")
	trace("returning to 'for' loop, or exiting now")


#-----------------------------------------------------------
# TRACING AND PRINTING FOR DEBUGGING
#-----------------------------------------------------------
def setTracing(traceState):
	global tracing
	tracing = traceState
	
def trace(message):
	global tracing
	if tracing:
		print message
#-----------------------------------------------------------
# END TRACING AND PRINTING FOR DEBUGGING
#-----------------------------------------------------------
setTracing(True)
main()