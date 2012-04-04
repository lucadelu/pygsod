#!/usr/bin/env python
#  class to download GSOD data
#
#  (c) Copyright Luca Delucchi 2012
#  Authors: Luca Delucchi
#  Email: luca dot delucchi at iasma dot it
#
##################################################################
#
#  This GSOD Python class is licensed under the terms of GNU GPL 2.
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License as
#  published by the Free Software Foundation; either version 2 of
#  the License, or (at your option) any later version.
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#  See the GNU General Public License for more details.
#
##################################################################

from datetime import *
import string
import os
import sys
import glob
import logging
import socket
import ftplib

class downGSOD:
    """A class to download GSOD data from FTP repository"""
    def __init__(self, 
                    password,
                    destinationFolder,
                    user = "anonymous",
                    url = "ftp.ncdc.noaa.gov",
                    stations = None,
                    file_stations = None
                    firstyear = 1928,
                    endyear = None,
                    debug = False
                ):
        """Initialization function :
            password = is your password, usually your email address
            destinationFolder = where the files will be stored
            user = your username, by default anonymous
            url = the url where to download the MODIS data
            stations = a list of stations code that you want to download, 
                        None == all stations, for search more info about stations
                        ftp://ftp.ncdc.noaa.gov/pub/data/gsod/ish-history.csv
            firstyear = the year to start downloading; by default 1928 that it is
                        the first year with some data
            endyear = the year to finish downloading; by default the current year
            debug = to see more info about downloading
            Creates a ftp instance, connects user to ftp server and goes into the 
            year directory where the GSOD data are stored
        """

        # url modis
        self.url = url
        # user for download
        self.user = user
        # password for download
        self.password = password
        # directory where data are collected
        self.path = "pub/data/gsod"
        # stations to downloads
        if stations:
            self.tiles = stations.split(',')
        elif file_stations:
            self.tiles = self.readFile(file_stations)
        else:
            self.tiles = None
        # set destination folder
        if os.access(destinationFolder,os.W_OK):
            self.writeFilePath = destinationFolder
        else:
            raise IOError("Folder to store downloaded files does not exist " \
            + " or is not writeable")
        # return the name of product
        if len(self.path.split('/')) == 2:
            self.product = self.path.split('/')[1]
        elif len(self.path.split('/')) == 3:
            self.product = self.path.split('/')[2]
        # write a file with the name of file downloaded
        self.filelist = open(os.path.join(self.writeFilePath, 'listfile' \
        + self.product + '.txt'),'w')
        # first year
        self.first = firstyear
        # force the last day
        self.end = endyear
        # for debug, you can download only xml files
        self.debug = debug
        # for logging
        LOG_FILENAME = os.path.join(self.writeFilePath, 'modis' \
        + self.product + '.log')
        LOGGING_FORMAT='%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG, \
        format=LOGGING_FORMAT)

    def readFile(self,filename):
        """ Read file to create list of station
            The file should be like this
            000040.99999
            000050.99999
            000090.99999
            000110.99999
            000140.99999
            ........
        """
        fn = open(filename)
        stations = [x.strip() for x in fn.readlines()]
        fn.close()
        return stations        
        
    def connectFTP(self):
        """ Set connection to ftp server, move to path where data are stored
        and create a list of directory for all days"""
        try:
            # connect to ftp server
            self.ftp = ftplib.FTP(self.url)
            self.ftp.login(self.user,self.password)
            # enter in directory
            self.ftp.cwd(self.path)
            self.dirData = []
            # return data inside directory
            self.ftp.dir(self.dirData.append)
            # reverse order of data for have first the nearest to today
            self.dirData.reverse()
            # check if dirData contain only directory, delete all files
            self.dirData = [elem.split()[-1] for elem in self.dirData if elem.startswith("d")]
            if self.debug==True:
                logging.debug("Open connection %s" % self.url)
        except EOFError:
            logging.error('Error in connection')
            self.connectFTP()

    def closeFTP(self):
        """ Close ftp connection """
        self.ftp.quit()
        self.filelist.close()
        if self.debug==True:
            logging.debug("Close connection %s" % self.url)

    def setDirectoryIn(self,year):
        """ Enter in the directory of the year """
        try:
            self.ftp.cwd(year)
            if self.debug==True:
                logging.debug("Enter in directory %s" % year)
        except (ftplib.error_reply,socket.error), e:
            logging.error("Error %s entering in directory %s" % e, year)
            self.setDirectoryIn(year)

    def setDirectoryOver(self):
        """ Come back to old path """
        try:
            self.ftp.cwd('..')
            if self.debug==True:
                logging.debug("Come back to directory")
        except (ftplib.error_reply,socket.error), e:
            logging.error("Error %s when try to come back" % e)
            self.setDirectoryOver()      

    def getListYears(self):
        """ Return a list of all selected years """  
        # set end year variable to the current year
        if self.end == None:
            self.end = date.today().year
        if self.debug==True:
            logging.debug("Start year %s, end year %s" % (self.first, self.end))
        rangeYears = range(self.first,self.end+1)
        return [str(year) for year in rangeYears if str(year) in self.dirData]
        
    def getFilesList(self,year):
        """ Create a list of files to download """ 
        # return all files in directory
        listfilesall = self.ftp.nlst()
        listfiles = []
        # if we pass some stations
        if self.tiles:
            # for each station
            for tile in self.tiles:
                # create name according to FTP style
                tname = "%s-%s.op.gz" % (tile, year)
                # if station exists for that year append to the download list
                if tname in self.listfilesall:
                    listfiles.append(tname)
        # without any stations it take all of them
        else:
            listfiles = listfilesall
        return listfiles

    def checkDataExist(self,listNewFile, move = 0):
        """ Check if a data already exists in the directory of download 
        Move serve to know if function is called from download or move function """
        fileInPath = []
        # add all files in the directory where we will save new modis data
        for f in os.listdir(self.writeFilePath):
            if os.path.isfile(os.path.join(self.writeFilePath, f)):
                fileInPath.append(f)
        # different return if this method is used from downloadsAllDay() or 
        # moveFile()
        if move == 0:
            listOfDifferent = list(set(listNewFile) - set(fileInPath))
        elif move == 1:
            listOfDifferent = list(set(fileInPath) - set(listNewFile))
        return listOfDifferent            
            
    def downloadFile(self,filDown,filSave):
        """ Download the single file """
        #try to download file
        try:
            self.ftp.retrbinary("RETR " + filDown, filSave.write)
            self.filelist.write("%s\n" % filDown)
            if self.debug==True:
                logging.debug("File %s downloaded" % filDown)
        #if it have an error it try to download again the file
        except (ftplib.error_reply,socket.error), e:
            logging.error("Cannot download %s, retry.." % filDown)
            self.connectFTP()
            self.downloadFile(filDown,filSave)

    def yearDownload(self,listFilesDown):
        """ Downloads stations for one year """
        # for each file in files' list
        for i in listFilesDown:
            fileSplit = i.split('.')
            # check if file exists on the output file
            oldFile = glob.glob1(self.writeFilePath, fileSplit[0] + "*")
            numFiles = len(oldFile)
            # if file doesn't exist download it
            if numFiles == 0:
                file_hdf = open(os.path.join(self.writeFilePath,i), "wb")
                self.downloadFile(i,file_hdf)
            # if file exists log an error
            elif numFiles == 1:
                logging.error("The file %s already exists" % i)
            elif numFiles > 1:
                logging.error("There are to much files for %s" % i)
                
    def allYears(self):
        """ Downloads stations for all years """
        listYears = self.getListYears()
        if self.debug==True:
            logging.debug("The number of years to download is: %i" % len(days))
        #for each year
        for year in listYears:
            #enter in the directory of year
            self.setDirectoryIn(year)
            #obtain list of all files
            listAllFiles = self.getFilesList(year)
            if listAllFiles:
                #obtain list of files to download
                listFilesDown = self.checkDataExist(listAllFiles)
                #download files for a year
                self.yearDownload(listFilesDown)
            self.setDirectoryOver()
        self.closeFTP()
        if self.debug==True:
            logging.debug("Download terminated")
        return 0        
                

