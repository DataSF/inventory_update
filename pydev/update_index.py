

# coding: utf-8

from optparse import OptionParser
from Utils import *
from ConfigUtils import *
import sys
from SocrataStuff import *
from PyLogger import *
from PandasUtils import *
from Emailer import *
from EmailComposer import *
from ScreenDoorStuff import *
import inflection as IN
from DictUtils import *
from JobStatusEmailerComposer import *

def parse_opts():
  helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig.yaml'
  parser = OptionParser(usage='usage: %prog [options] ')
  parser.add_option('-c', '--configfile',
                      action='store',
                      dest='configFn',
                      default=None,
                      help=helpmsgConfigFile ,)

  helpmsgConfigDir = 'Use the -d to add directory path for the config files. EX: /home/ubuntu/configs'
  parser.add_option('-d', '--configdir',
                      action='store',
                      dest='configDir',
                      default=None,
                      help=helpmsgConfigDir ,)
  helpmsgjobType = 'Use the -n to specify a job name. EX: profile_fields - can either be profile_datasets or profile_fields'
  parser.add_option('-n', '--jobtype',
                      action='store',
                      dest='jobType',
                      default=None,
                      help=helpmsgjobType ,)

  (options, args) = parser.parse_args()

  if  options.configFn is None:
    print "ERROR: You must specify a config yaml file!"
    print helpmsgConfigFile
    exit(1)
  elif options.configDir is None:
    print "ERROR: You must specify a directory path for the config files!"
    print helpmsgConfigDir
    exit(1)
  elif options.jobType is None:
    print "ERROR: You must specify a directory path for the config files!"
    print helpmsgjobType
    exit(1)

  config_inputdir = None
  fieldConfigFile = None
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  jobType =  options.jobType
  return fieldConfigFile, config_inputdir, jobType



qry department_or_division
