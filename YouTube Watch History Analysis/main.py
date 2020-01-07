from watchHistory import WatchRecord, WatchProcessor, Data
import logging

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "log_main.log",
                    level = logging.DEBUG,
                    format = LOG_FORMAT,
                    filemode = 'w')

logger = logging.getLogger()

def main():
    # load the data into an object
    data = Data("watch-history.json", "subscriptions.json", "videoDict.json")
    
    # setup the watchHistory df
    watchHistory = WatchProcessor(data.watchHistoryList, data.videoDict)
    
    #add the other columns

    watchHistory.addPublishedAt()
    
    # watchHistory.addColumn('duration')
    # watchHistory.addColumn('publishedAt')
    # watchHistory.addColumn('channelTitle')
    # watchHistory.df.to_csv("watchHistoryDF.csv")



main()
