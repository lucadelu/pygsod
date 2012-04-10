#!/usr/bin/env python
# script to convert GSOD file to csv or sql file
#
#  (c) Copyright Antonio Galea, 2009, per FEM-CEALP
#  Authors: Antonio Galea, Luca Delucchi
#  Email: luca dot delucchi at fmach dot it
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
# 

import sys
import os.path
import re

from optparse import OptionParser

def f2c(temperature):   return "%.1f" % ((float(temperature) - 32.) / 1.8)
def miles2km(distance): return "%.1f" % (float(distance) / .6214)
def knots2kmh(speed):   return "%.1f" % (float(speed) / 1.9425)
def inches2mm(length):  return "%.1f" % (float(length) * 25.4 * 0.1)

input_format = [
    #name, start, end, conversion function, sql type
    ('stn',1,6,None,'INTEGER'),
    ('wban',8,12,None,'INTEGER'),
    ('year',15,18,None,'INTEGER'),
    ('month',19,20,None,'INTEGER'),
    ('day',21,22,None,'INTEGER'),
    ('temp',25,30,f2c,'FLOAT'),         #temperature; Fahrenheit
    ('temp_count',32,33,None,'INTEGER'),
    ('dewp',36,41,f2c,'FLOAT'),         #dew point; Fahrenheit
    ('dewp_count',43,44,None,'INTEGER'),
    ('slp',47,52,None,'FLOAT'),         #sea level pressure; millibars
    ('slp_count',54,55,None,'INTEGER'),
    ('stp',58,63,None,'FLOAT'),         #station pressure; millibars
    ('stp_count',65,66,None,'INTEGER'),
    ('visib',69,73,miles2km,'FLOAT'),   #visibility; miles
    ('visib_count',75,76,None,'INTEGER'),
    ('wdsp',79,83,knots2kmh,'FLOAT'),   #wind speed; knots
    ('wdsp_count',85,86,None,'INTEGER'),
    ('mxspd',89,93,knots2kmh,'FLOAT'),  #maximum sustained wind speed; knots
    ('gust',96,100,knots2kmh,'FLOAT'),  #maximum wind gust; knots
    ('max',103,108,f2c,'FLOAT'),        #maximum temperature; Fahrenheit
    ('max_flag',109,109,None,'CHAR(1)'),
    ('min',111,116,f2c,'FLOAT'),        #minimum temperature; Fahrenheit
    ('min_flag',117,117,None,'CHAR(1)'),
    ('prcp',119,123,inches2mm,'FLOAT'), #total precipitation (rain/melted snow); inches
    ('prcp_flag',124,124,None,'CHAR(1)'),
    ('sndp',126,130,inches2mm,'FLOAT'), #snow depth; inches
    #the following are flags: 1 yes, 0 no
    ('fog',133,133,None,'INTEGER'),
    ('rain',134,134,None,'INTEGER'),      #rain or drizzle
    ('snow',135,135,None,'INTEGER'),      #snow or ice pellets
    ('hail',136,136,None,'INTEGER'),     
    ('thunder',137,137,None,'INTEGER'),
    ('tornado',138,138,None,'INTEGER'),   #tornado or funnel cloud
]
missing_data = re.compile('^9+\.9+$')
pkey_fields = ('stn', 'wban', 'year', 'month', 'day')
def threshold_check(lst,threshold):
    d = dict([ (field,value) for ((field,start,end,conv,type),value) in zip(input_format,lst) ])
    if d['temp_count']<threshold:  d['temp']=None
    if d['dewp_count']<threshold:  d['dewp']=None
    if d['slp_count']<threshold:   d['slp']=None
    if d['stp_count']<threshold:   d['stp']=None
    if d['visib_count']<threshold: d['visib']=None
    if d['wdsp_count']<threshold:  d['wdspd']=None
    return [ d[field] for (field,start,end,conv,type) in input_format ]

def parse(fname,validate=None):
    try:
        lines = file(fname).readlines()
    except IOError,e:
        print e
        sys.exit(1)
    values = []
    for line in lines[1:]:
        tmp = []
        for (field,start,end,conv,type) in input_format:
            value = line[start-1:end].strip()
            if missing_data.match(value): value = None
            elif conv: value = conv(value)
            tmp.append(value)
        if validate: tmp = validate(tmp)
        values.append(tmp)
    return values

def output_csv(values,separator):
    def coalesce(v,n):
        if v != None: return v
        return n
    print separator.join([field for (field,start,end,conv,type) in input_format])
    for lst in values:
        print separator.join([ coalesce(value,"") for value in lst ])

def output_sql(values,tbl,create):
    fields = [ (field,type) for (field,start,end,conv,type) in input_format ]
    if create:
        print "CREATE TABLE %s (\n %s,\n PRIMARY KEY (%s)\n);" % (
        	tbl,",\n ".join([ "%s %s" % (f,t) for (f,t) in fields]),
        	", ".join([p for p in pkey_fields])
        )
    text = re.compile('char',re.I)
    for lst in values:
        f = []
        v = []
        for ((field,type),value) in zip(fields,lst):
            if value != None: 
                f.append(field)
                if text.match(type): value = "'%s'" % value
                v.append(value)
        print "INSERT INTO %s (%s) VALUES (%s);" % (tbl,",".join(f),",".join(v))

if __name__ == "__main__":
    mode_choices = ['csv','sql']
    parser = OptionParser("Usage: %prog [options] filename")
    parser.set_defaults(
        mode='csv',
        separator=',',
        tablename='myTable',
        createtable=False,
        threshold=0 
    )
    parser.add_option("-m", "--mode", action="store", choices=mode_choices, 
                     default='csv', help="one of %s" % ",".join(mode_choices) \
                     +" [default=%default]")
    parser.add_option("-s", "--separator", action="store", default=',',
                     help="separator character [used in csv mode only, default='%default']")
    parser.add_option("-n", "--tablename", action="store", default='gsod',
                     help="tablename used in INSERT statements " \
                     + "[used in sql mode only, default=%default]")
    parser.add_option("-c", "--createtable", action="store_true", 
                     help="add sql instruction for creating the table [used in sql mode only]")
    parser.add_option("-t", "--threshold", action="store", type="int", default=0,
                    help="data is valid only if reported at least threshold" \
                    + " times (default %default = always valid)")
    (options, args) = parser.parse_args()

    if not args:
        parser.error('missing filename')
        sys.exit(1)

    if options.threshold>0:
        validation_function = lambda x: threshold_check(x,options.threshold)
    else:
        validation_function = None

    fname=args[0]
    values = parse(fname,validation_function)
        
    if options.mode == 'csv':
        output_csv(values,options.separator)
    elif options.mode == 'sql':
        output_sql(values,options.tablename,options.createtable)

