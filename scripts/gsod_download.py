#!/usr/bin/env python
# script to download massive GSOD data from ftp
#
#  (c) Copyright Luca Delucchi 2012
#  Authors: Luca Delucchi
#  Email: luca dot delucchi at iasma dot it
#
##################################################################
#
#  This GSOD Python script is licensed under the terms of GNU GPL 2.
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

#import system library
import sys
import optparse
from datetime import *
#import modis library
from pygsod import downgsod

#classes for required options
strREQUIRED = 'required'

class OptionWithDefault(optparse.Option):
    ATTRS = optparse.Option.ATTRS + [strREQUIRED]
    
    def __init__(self, *opts, **attrs):
        if attrs.get(strREQUIRED, False):
            attrs['help'] = '(Required) ' + attrs.get('help', "")
        optparse.Option.__init__(self, *opts, **attrs)

class OptionParser(optparse.OptionParser):
    def __init__(self, **kwargs):
        kwargs['option_class'] = OptionWithDefault
        optparse.OptionParser.__init__(self, **kwargs)
    
    def check_values(self, values, args):
        for option in self.option_list:
            if hasattr(option, strREQUIRED) and option.required:
                if not getattr(values, option.dest):
                    self.error("option %s is required" % (str(option)))
        return optparse.OptionParser.check_values(self, values, args)

def main():
    """Main function"""
    #usage
    usage = "usage: %prog [options] destination_folder"
    parser = OptionParser(usage=usage)
    #password
    parser.add_option("-P", "--password", dest="password",
                      help="password for connect to ftp server", required=True)
    #username
    parser.add_option("-U", "--username", dest="user", default = "anonymous",
                      help="username for connect to ftp server")
    #url
    parser.add_option("-u", "--url", default = "ftp.ncdc.noaa.gov",
                      help="ftp server url [default=%default]", dest="url")
    #stations
    parser.add_option("-s", "--stations", dest="stations", default=None,
                      help="string of stations' code separated from comma " \
                      + "[default=%default for all stations]")
    #file of stations
    parser.add_option("-F", "--file", dest="fstations", default=None,
                      help="path to file containing list of stations' code " \
                      + "[default=%default for all stations]")                      
    #first day
    parser.add_option("-f", "--firstyear", dest="today", default=1928,
                      metavar="FIRST_YEAR", help="the first year to start download " \
                      + "[default=%default is 1928]; if you want change" \
                      " year you must use this format YYYY")
    #first day
    parser.add_option("-e", "--endyear", dest="enday", default=date.today().year,
                      metavar="LAST_YEAR", help="the last year to finish download " \
                      + "[default=%default]; if you want change" \
                      " year you must use this format YYYY")
    #debug
    parser.add_option("-x", action="store_true", dest="debug", default=True,
                      help="this is useful for debug the download")
    #set false several options
    parser.set_defaults(debug=False)

    #return options and argument
    (options, args) = parser.parse_args()
    #test if args[0] it is set
    if len(args) == 0:
        parser.error("You have to pass the destination folder for GSOD file")

    #set modis object
    gsodOgg = downgsod.downGSOD(url = options.url, user = options.user, 
        password = options.password, destinationFolder = args[0], 
        stations = options.stations, file_stations = options.fstations,  
        firstyear=options.today, endyear = options.enday, debug = options.debug)
    #connect to ftp
    gsodOgg.connectFTP()
    #download data
    gsodOgg.allYears()
    

#add options
if __name__ == "__main__":
    main()