#!/usr/bin/python
# -*- coding: utf-8 -*-
#  installation file for gsodpy
#
#  (c) Copyright Luca Delucchi 2012
#  Authors: Luca Delucchi
#  Email: luca dot delucchi at fmach dot it
#
##################################################################
#
#  Modis class is licensed under the terms of GNU GPL 2     
#  This program is free software; you can redistribute it and/or 
#  modify it under the terms of the GNU General Public License as 
#  published by the Free Software Foundation; either version 2 of 
#  the License,or (at your option) any later version.       
#  This program is distributed in the hope that it will be useful,  
#  but WITHOUT ANY WARRANTY; without even implied warranty of   
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.     
#  See the GNU General Public License for more details. 
#
##################################################################
from distutils.core import setup

setup(
  name = 'pygsod',
  version = '0.1.0',
  py_modules = ['pygsod.downgsod'],
  scripts = ['scripts/gsod_download.py'],
  author = 'Luca Delucchi',
  author_email = 'luca.delucchi@iasma.it',
  url = 'http://gis.fem-environment.eu/gis-development/pygsod',
  description = 'Python library for GSOD data',
  long_description = 'Python library to download GSOD data',
  license = 'GNU GPL 2 or later'
)