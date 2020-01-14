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
#
# 10 June 2019
# Joe Schneider / joe.schneider@samsara.com
#
#
# Purpose: pull Samsara driver data and output CSV of each driver's active, driving, and on duty time for a given time frame

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

def get_times(receivedDate):
    firstMidnight = dt.datetime.combine(receivedDate, dt.time.min)
    secondMidnight = firstMidnight + timedelta(days=1)
    startMs =  int(time.mktime(firstMidnight.timetuple()) * 1000)+1
    endMs = int(time.mktime(secondMidnight.timetuple()) * 1000)-1
    #print startMs
    #print endMs
    return (startMs, endMs)

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

	
def getlogs(token, group, driverid, name, startDate, endDate, deltaDays, csvfile):
    d={}
    for i in range(deltaDays):
        times=get_times(startDate+ timedelta(days=i))
        startMs=times[0]
        endMs=times[1]
        payload = {'access_token':token, 'startMs':str(startMs), 'endMs':str(endMs)}
        params = ( ('access_token', token),)
        postdata = '{"groupId":'+group+',"startMs":'+str(startMs)+',"endMs":'+str(endMs)+'}'
        logs = requests.post('https://api.samsara.com/v1/fleet/drivers/'+str(driverid)+'/hos_daily_logs',
           params=params, data=postdata)
        #data = json.loads(logs.text)
        if not logs.text:
            print "Error log", logs
            d=0
            return d
        try:
            data = json.loads(logs.text)
            for log in data['days']:
                d[i]={"activeMs":log['activeMs'], "drivingTime":log['driveMs'], "onDutyMs":log['onDutyMs'], "distanceMiles":log['distanceMiles'], "startMs":log['startMs']}
        except:
            print "failed getting logs"
        selectedDate=d[i]['startMs']/1000
        activeMinutes=d[i]['activeMs']/1000/60
        drivingMinutes=d[i]['drivingTime']/1000/60
        onDutyMinutes=d[i]['onDutyMs']/1000/60
        csvfile.writerow({'Day':(datetime.utcfromtimestamp(selectedDate).strftime('%m-%d-%Y')),'Driver':name, 'Active Time (minutes)':activeMinutes, 'Driving Time (minutes)':drivingMinutes, 'On Duty Time (minutes)':onDutyMinutes, 'Distance (miles)':d[i]['distanceMiles']})

        i=i+1			
    return 0

    

def main():
    alldrivers = getdrivers(config.token, config.group)
    startMonth=config.startMonth
    endMonth=config.endMonth
    startDay=config.startDay
    endDay=config.endDay
    configYear=config.year
    startDate = datetime(year=int(configYear, 10), month=int(startMonth, 10), day=int(startDay, 10))
    endDate = datetime(year=int(configYear, 10), month=int(endMonth, 10), day=int(endDay, 10))
    deltaDays=abs((endDate - startDate).days)+1
    print "Found: %d drivers" % len (alldrivers)
    commuterfile= dt.datetime.strftime(startDate, '%m-%d-%Y')+" - "+ dt.datetime.strftime(endDate, '%m-%d-%Y')+ '.csv'
    outCommute=open(commuterfile, 'w') 
    with outCommute:
        myFields=["Day","Driver","Active Time (minutes)", "Driving Time (minutes)", "On Duty Time (minutes)", "Distance (miles)"]
        commutewriter = csv.DictWriter(outCommute, fieldnames=myFields,lineterminator = '\n')
        commutewriter.writeheader()
        for driverid,name in alldrivers.items():
            logs = getlogs(config.token, config.group, driverid, name, startDate, endDate, deltaDays, commutewriter)
if __name__ == "__main__":
    main()