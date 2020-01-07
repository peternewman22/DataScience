import pandas as pd
import numpy as np
import os
import seaborn as sns
import json
from urllib.parse import urlparse, quote, urlencode
import requests
from datetime import datetime, timedelta
import re
import logging

# setting up logging
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "log_watch-history_py.log",
                    level = logging.DEBUG,
                    format = LOG_FORMAT,
                    filemode = 'w')

logger = logging.getLogger()

class Data:
    def __init__(self, watchHistoryJson, subscriptionJson, videoDictJson):
        self.watchHistoryList = self.load(watchHistoryJson)
        self.subscriptionsDict = self.load(subscriptionJson)
        self.videoDict = self.load(videoDictJson)

    def load(self, jsonFile):
        with open(jsonFile, "r", encoding='utf') as fh:
            return json.load(fh)


class WatchRecord:
    """Processes each WatchRecord and return a row for processing"""
    def __init__(self, url, time):
        self.url = url
        self.id = self.url[32:]
        self.time = self.getTime(time)
        "https://www.youtube.com/watch?v="

    def getTime(self, timeStr):
        format_ = "%Y-%m-%dT%H:%M:%S"
        try:
#           # adjusting for timedifference
            time = datetime.strptime(timeStr[:19], format_) + timedelta(hours=11)
            return time
        except Exception as e:
            logger.error(f"Problem with getTime... {timeStr}: {e}")

    def row(self):
        return {'url' : self.url, 'time' : self.time, 'id' : self.id}
            

        
class WatchProcessor:
    """Processes Watch-Data
    1. Sets up rows of clean data
    2. Constructs a dataframe from the rows
    3. Requests other data from YouTube API
    4. Generates various graphs etc
    """
    def __init__(self, watchRecordList, videoDict):
        self.cleanData = self.getCleanData(watchRecordList)
        self.df = pd.DataFrame(self.cleanData)
        self.videoDict = videoDict
        

    def getCleanData(self, watchRecordList):
        clean_data = []
        for eachRecord in watchRecordList:
            try:
                toAppend = WatchRecord(eachRecord['titleUrl'], eachRecord['time']).row()
                clean_data.append(toAppend)
            except Exception as e:
                logger.error(f"Appending to clean_data: {e}")
        return clean_data

    def getTime(self, timeStr):
        format_ = "%Y-%m-%dT%H:%M:%S"
        try:
#           # adjusting for timedifference
            time = datetime.strptime(timeStr[:19], format_) + timedelta(hours=11)
            return time
        except Exception as e:
            logger.error(f"Problem with {timeStr}: {e}")
            return "missing"
            
    def addPublishedAt(self):
        self.df['publishedAt'] = self.df.apply(lambda row: self.getPublishedAt(row), axis = 1),
        
    def addChannelTitle(self):
        self.df['channelTitle'] = self.df.apply(lambda row: self.getChannelTitle(row), axis=1),
        
    def addTitle(self):
        self.df['title'] = self.df.apply(lambda row: self.getTitle(row), axis=1),
    
    def addDuration(self):
        self.df['duration'] = self.df.apply(lambda row: self.getDuration(row), axis = 1)
        

    def getTitle(self, row):
        try:
            return self.videoDict[row['id']]['title']
        except Exception as e:
            logger.error(f"Problem adding Title for row {row}: {e}")
            return "Missing" 

    def getPublishedAt(self, row):
        try:
            vidToSearch = row['id']
            timeStr = self.videoDict[vidToSearch]['publishedAt']
            return self.getTime(timeStr)
        except Exception as e:
            logger.error(f"Problem adding publishedAt for row {row}: {e}")
            return "Missing"

    def getChannelTitle(self, row):
        try:
            return self.videoDict[row['id']]['channelTitle']
        except Exception as e:
            logger.error(f"Problem adding channelTitle for row {row}: {e}")
            return "Missing"

    def getDuration(self, row):
        try:
            return DurationProcessor(self.videoDict[row['id']]['duration']).time
        except Exception as e:
            logger.error(f"Problem adding duration for row {row}: {e}")
            return "Missing"

class DurationProcessor:
    def __init__(self, timeStr):
        self.timeStr = timeStr
        self.time = self.getTime(self.timeStr)

    def getTime(self, timeStr):
        hours = re.findall(r"(\d+)H",timeStr)
        minutes = re.findall(r"(\d+)M",timeStr)
        seconds = re.findall(r"(\d+)S",timeStr)
        return timedelta(hours = self.testIfMissing(hours), minutes = self.testIfMissing(minutes), seconds = self.testIfMissing(seconds))

    def testIfMissing(self, t):
        if len(t) != 0:
            return int(t[0])
        if len(t) == 0:
            return 0
    
"""
class BatchProcessor:
     def __init__(self, batch, batchNumber, APIkey):
         self.batchNumber = batchNumber
         self.batchId = ','.join(x for x in batch)
         self.snippet_params = {'part' : 'snippet', 'id' : batchId, 'key': APIkey}
         self.duration_params = {'part' : 'contentDetails', 'id' : batchId, 'key': creds["APIkey"]}
         self.snippetAPIUrl = "https://www.googleapis.com/youtube/v3/videos?"+urlencode(snippet_params)               
         self.duration_api_url = "https://www.googleapis.com/youtube/v3/videos?"+urlencode(duration_params)

    def processSnippetRequest(self):
        #Requests snippet from YouTube API v3
        snippetRequestJson = requests.get(self.snippetAPIUrl).json()
        logger.debug("Got snippet request: \n{snippetRequestJson} from batch {self.batchNumber}")
        for eachItem in self.SnippetRequestJson['items']:
            videoDict[eachItem['id']] = {'title' : eachItem['snippet']['title'],
                                        'publishedAt' : eachItem['snippet']['publishedAt'],
                                        'channelTitle' : eachItem['snippet']['channelTitle']
                                        }
    
    def processDurationRequest(self):
        durationRequestJson"""