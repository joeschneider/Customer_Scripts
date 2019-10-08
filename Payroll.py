#
#/*************************************************************************
# Copyright (c) 2019, Samsara Networks Inc.
# All Rights Reserved.
# NOTICE:  All code and technical concepts contained herein ("Software") is, and remains, the property of Samsara Networks Inc. and its suppliers, if any.  The Software is proprietary to Samsara
# Networks Inc, and is protected by patent, trade secret, and copyright law.  
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
#*******************************************************************************
# Legend Energy Services HOS 
#
# 29 May 2019
# Joe Schneider / joe.schneider@samsara.com
#
#
# Purpose: pull Samsara API HOS data and output CSV of each working shift a driver took the previous day

import datetime as dt
import time
from datetime import timedelta
from datetime import datetime
import config
import requests
import json
import collections
import unicodecsv as csv
import sys
#python -m pip install unicodecsv


debug=False

def get_times():
    midnight = dt.datetime.combine(dt.datetime.today(), dt.time.min)
    yesterday_midnight = midnight - timedelta(days=1)
    startms =  int(time.mktime(yesterday_midnight.timetuple()) * 1000)+1
    endms = int(time.mktime(midnight.timetuple()) * 1000)-1
    return (startms, endms)

def getdrivers(token, group):
    d={}
    params = ( ('access_token', token),)
    groupId = group
    postdata = '{"groupId":'+group+'}'
    driver = requests.post('https://api.samsara.com/v1/fleet/drivers',
           params=params, data=postdata)
    data = json.loads(driver.text)
    for driver in data['drivers']:
        driverid=driver['id']
        d[driverid]=driver['name']
    return d

def getdriverusername(token, driverid):
    d={}
    params = ( ('access_token', token),)
    driver = requests.get('https://api.samsara.com/v1/fleet/drivers/'+str(driverid),
           params=params)
    data = json.loads(driver.text)
    return data["username"]

def get_vehicles(token,group):
    d={}
    params = ( ('access_token', token),)
    groupId = group
    postdata = '{"groupId":'+group+'}'
    vehiclelist = requests.post('https://us5.api.samsara.com/v1/fleet/list', 
            params=params, data=postdata)
    data = json.loads(vehiclelist.text)
    for vehicle in data['vehicles']:
        vid=str(vehicle['id'])
        d[vid]=vehicle['name']
    return d

	#pretty much the same as GetLogs()
def getEndOfPreviousDay(driverid, token, group):
    midnight = dt.datetime.combine(dt.datetime.today() - timedelta(days=1), dt.time.min)
    yesterday_midnight = midnight - timedelta(days=1)
    startms =  int(time.mktime(yesterday_midnight.timetuple()) * 1000)+1
    endms = int(time.mktime(midnight.timetuple()) * 1000)-1
    d={}
    params = ( ('access_token', token),)
    groupId = group
    postdata = '{"groupId":'+group+',"driverID":'+str(driverid)+',\
    "startMs":'+str(startms)+',"endMs":'+str(endms)+'}'
    logs = requests.post('https://api.samsara.com/v1/fleet/hos_logs',
           params=params, data=postdata)
    if not logs.text:
        print "Error log", logs
        d=0
        return d
    try:
        data = json.loads(logs.text)
        i=0;
        for log in data['logs']:
            d[i]={"startms":log['logStartMs'],"status":log['hosStatusType'],\
            "vid":log['vehicleId'],"remark":log['remark']}
            i=i+1
        return d
    except:
        print "failed getting logs"
        return 0
	
def getlogs(token, group, driverid, startms, endms):
    d={}
    params = ( ('access_token', token),)
    groupId = group
    postdata = '{"groupId":'+group+',"driverID":'+str(driverid)+',\
    "startMs":'+str(startms)+',"endMs":'+str(endms)+'}'
    logs = requests.post('https://api.samsara.com/v1/fleet/hos_logs',
           params=params, data=postdata)
    if not logs.text:
        print "Error log", logs
        d=0
        return d
    try:
        data = json.loads(logs.text)
        i=0;
        for log in data['logs']:
            d[i]={"startms":log['logStartMs'],"status":log['hosStatusType'],\
            "vid":log['vehicleId'],"remark":log['remark']}
            i=i+1
        return d
    except:
        print "failed getting logs"
        return 0

def writeCSVrows(timein, timeout,name,remarkin, remarkout, csvfile):
    FMT = '%Y-%m-%d %H:%M:%S'
    times=get_times()
    regulartimeout=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timeout/1000))
    regulartimein=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timein/1000))
    difference=datetime.strptime(regulartimeout, FMT) - datetime.strptime(regulartimein, FMT)
    if timein>=times[0]:
        csvfile.writerow({'Duration': difference,"Punch in":regulartimein, "Punch out":regulartimeout,"Name":name, "Punch In Remark":remarkin,"Punch Out Remark":remarkout})
    else:
        print "Log in time: %s, Log out time: %s" % (timein,timeout)
        csvfile.writerow({'Duration': "VOID-Driver is disconnected","Punch in":"Void", "Punch out":"Void","Name":name, "Punch In Remark":remarkin,"Punch Out Remark":remarkout})
    return
		
def processlogs(driverid, logs,vehicles,name,username,commutew):
    oldstatus=''
    remark=''
    remarkIn=''
    lastStatusOfLastDay=''
    times=get_times()
    timein=0
    timeout=0
    counter=0
    csvfile=commutew
    firstStatus=''
    FMT = '%Y-%m-%d %H:%M:%S'
	#Call to see the driver's logs of the previous day is
    lastLog=getEndOfPreviousDay(driverid, config.token, config.group)
    if(len(lastLog))==0:
        lastStatusOfLastDay="OFF_DUTY"
    else:
        for k,log in lastLog.items():
                if k==(len(lastLog)-1):
				    #their last status of yesterday=first status of today
                    lastStatusOfLastDay=log['status']
    #skip any blank logs
    if len(logs)<1:
        return
    elif len(logs)==1:
		#switched from Off_duty to something else, indicating that they are working from some point in the day through midnight
		#Sometimes I've found even when the driver is in Off Duty status, there's an extra off duty status showing up, hence the second part of the conditional
        if (lastStatusOfLastDay=='OFF_DUTY' or lastStatusOfLastDay=="SLEEPER_BED") and lastStatusOfLastDay!=log['status']:
            timeout=times[1]
            timein=log['startms']
            remarkIn=log['remark']
            remark="End of Day"
            print "1"
            writeCSVrows(timein,timeout,name, remarkIn,remark, csvfile)
		#checking to see if the status is NOW off-duty or sleeper, indicating that they weren't off-duty at the start of the day, but end that way
        elif (log['status']=='OFF_DUTY' or log['status']=="SLEEPER_BED") and lastStatusOfLastDay!=log['status']:
            timeout=log['startMs']
            timein=times[0]
            remarkIn="Started Previous Night"
            remark=log['remark']
            print "2"
            writeCSVrows(timein,timeout,name, remarkIn,remark, csvfile)
		#return so as to not fall into one of the other possible options
        return
	
	
	####multiple status changes####
	
	
    #checking to see if they started the day Off Duty
    if lastStatusOfLastDay=='OFF_DUTY' or lastStatusOfLastDay =='SLEEPER_BED':
        oldstatus=lastStatusOfLastDay
        for k,log in logs.items():
		    #if they go from off duty/sleeper to on/driving, mark the start time, punch in remark, and increment the counter
            if (oldstatus=='OFF_DUTY' or oldstatus=='SLEEPER_BED') and(log['status']=='DRIVING' or log['status']=='ON_DUTY') and oldstatus!=log['status']:
                counter=counter+1
                timein=log['startms']
                remarkIn=log['remark']
			#if they switch to off duty, mark the time out, increment the counter, and write the row
            elif (log['status']=='OFF_DUTY' or log['status']=="SLEEPER_BED") and oldstatus!=log['status']:
                counter=counter+1
                timeout=log['startms']
                remark=log['remark']
                print "3"
                writeCSVrows(timein,timeout,name, remarkIn,remark, csvfile)
            oldstatus=log['status']
		#if the counter is an odd number at the end of the day, it means the driver started off duty, and ended while driving or off duty, and that row is added to the CSV
		#the last row for the driver should extend to the end of the day
        if counter%2==1:
            remark="End of Day"
            timeout=times[1]
            print "4"
            writeCSVrows(timein,timeout,name, remarkIn,remark, csvfile)
        return
    else:
        oldstatus=lastStatusOfLastDay
	    #driver started in either Drive or On Duty status
        for k,log in logs.items():
		    #when they switch to off duty, incrememnt the counter, and punch out time
            if (log['status']=='OFF_DUTY' or log['status']=="SLEEPER_BED") and oldstatus!=log['status']:
                counter=counter+1
                timeout=log['startms']
                remark=log['remark']
				#if it's the first time they go off duty during the day, the start time should reflect midnight
                if counter==1:
                    timein=times[0]
                    remarkIn="Started Previous Night"
                    print "5"
                    writeCSVrows(timein,timeout,name, remarkIn, remark, csvfile)
				#if they go off duty and it's not the first time, the counter
                else:
                    print "6"
                    writeCSVrows(timein,timeout,name, remarkIn,remark, csvfile)
		    #when the driver goes back on duty, increment the counter and record the punch-in time and remark
            elif (oldstatus=='OFF_DUTY' or oldstatus=="SLEEPER_BED")and(log['status']=='DRIVING' or log['status']=='ON_DUTY'):
                counter=counter+1
                timein=log['startms']
                remarkIn=log['remark']
            oldstatus=log['status']
		#if the counter is an even number at the end of the day, the driver finished the day while on duty and 11:59 will be their punch out time
        if counter%2==0:
            timeout=times[1]
            remark="End of day"
            print "7"
            writeCSVrows(timein,timeout,name, remarkIn, remark, csvfile)
        return

def main():
    times=get_times()
    vehicles=get_vehicles(config.token, config.group)
    alldrivers = getdrivers(config.token, config.group)
    print "Found: %d drivers" % len (alldrivers)
    commuterfile= dt.datetime.strftime(dt.datetime.now() - timedelta(1), '%Y-%m-%d')+'.csv'
    outCommute=open(commuterfile, 'w')
    with outCommute:
        myFields=["Duration","Punch in","Punch out","Name","Punch In Remark", "Punch Out Remark"]
        commutewriter = csv.DictWriter(outCommute, fieldnames=myFields,lineterminator = '\n')
        commutewriter.writeheader()
        for driverid,name in alldrivers.items():
            print "Driver is %s" % name
            username=getdriverusername(config.token, driverid)
            logs = getlogs(config.token, config.group, driverid, times[0], times[1])
            sortedlogs = collections.OrderedDict(sorted(logs.items()))
            processlogs(driverid,sortedlogs,vehicles,name,username,commutewriter)
if __name__ == "__main__":
    main()