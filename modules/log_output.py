# --------------------------------------------------------------------------------------------------------------------------------
# Name:        log_output
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2022
# Copyright:   (c) jennifer.mcclelland 2022
#
# ----This class offers methods to create a logfile named with the current data and time.
# -----An interface is provided to add content to the logfile at any point in the processing.
# -----An error flag and error content holder can be set and derived to indicate if there were processing errors
# -----and the specific error content. In case of an error, this is added to the logfile before finalization.
# --------------------------------------------------------------------------------------------------------------------------------


import os
import datetime
import csv
from pathlib import Path


class LogOutput:

    __fileHandlerLog = None
    __fileHandlerWriter = None
    __fileHandlerCsv = None
    __userSettings = None
    __error = False
    __lastErrorMessage = ""
    __totalTime = 0
    __amountScenes = 0
    __userSettings = None
    logfile_dir = ""

    def __init__(self, user_settings):

        self.__userSettings = user_settings

        current_dir = Path.cwd()
        parent_dir = current_dir.parent
        self.logfile_dir = str(parent_dir) + "/log_output_agri_ts_gen/"

        # Create log output folder if not available
        if not os.path.exists(self.logfile_dir):
            os.makedirs(self.logfile_dir)

    def createLogOutputFile(self, prefixName):

        currentDatetime = datetime.datetime.now()
        date = ("%s%s%s" % (currentDatetime.day, currentDatetime.month, currentDatetime.year))
        time = ("%s:%s:%s" % (currentDatetime.hour, currentDatetime.minute, currentDatetime.second))

        self.__error = False
        fileName = self.logfile_dir + "log_" + date + "_" + time + "_" + prefixName +".txt"
        procTimesName = self.logfile_dir + "proc_times_" + date + "_" + time + "_" + \
                        prefixName + ".csv"

        print("Log file created at: " + fileName)
        print("Rasdaman benchmarking at: " + procTimesName)

        self.__fileHandlerLog = open(fileName, 'a')
        self.__fileHandlerCsv = open(procTimesName, 'a')
        self.__fileHandlerWriter = csv.writer(self.__fileHandlerCsv)

        self.__fileHandlerWriter.writerow(["Field", "Time", "Total Amount"])


    def appendOutputToLog(self, content, error=False):
        if self.__fileHandlerLog is not None:
            self.__fileHandlerLog.write(content + "\n")
            self.__fileHandlerLog.flush()
        if error:
            self.__error = error

    def appendProcTime(self, aoiName, time):
        self.__amountScenes = self.__amountScenes + 1
        self.__totalTime = self.__totalTime + time
        self.__fileHandlerWriter.writerow([aoiName, time, self.__amountScenes])
        self.__fileHandlerCsv.flush()

    def setTotalTime(self):
        self.__fileHandlerWriter.writerow([self.__amountScenes, self.__totalTime])

        output = "The total processing time for all scenes is: %s sec" % self.__totalTime
        self.__fileHandlerLog.write(output + "\n")
        self.__totalTime = 0
        print(output)

    def closeCurrentFiles(self):
        if self.__error:
            self.__fileHandlerLog.write("-----------------------Attention:Processing contains Error!!!------------------" + "\n")
        else:
            self.__fileHandlerLog.write("-----------------------Processing successful!!!------------------" + "\n")
        self.__fileHandlerLog.close()
        self.__fileHandlerCsv.close()

    def setCurrentErrorMsg(self, msg):
        self.__lastErrorMessage = msg

    def getCurrentErrorMsg(self):
        return self.__lastErrorMessage

    def getError(self):
        return self.__error

    def setError(self):
        self.__error = True
