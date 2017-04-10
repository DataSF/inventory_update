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


def main():
  fieldConfigFile, config_inputdir, jobType =  parse_opts()
  configItems = ConfigUtils.setConfigs(config_inputdir, fieldConfigFile )
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sQobj = SocrataQueries(clientItems, configItems, logger)
  email_list =  DatasetUtils.getDatasetAsDictListPageThrough(configItems['dd']['index']['fbf'], sQobj,configItems['dd']['index']['qryCols'])

  e = Emailer(configItems)
  ec = EmailComposer(configItems, e)
  base_email_txt = ec.getBaseMsgText('systems_updt')
  subject_line = e._emailConfigs['email_situations']['systems_updt']['subject_line']
  wkbks_dir = configItems['wkbk_dir']+ '/blank_wkbks/'
  for item in email_list:
    msgBody  = base_email_txt % (item['coordinator_name'], item['template_file_name'])
    attachment_dictList = [{item['template_file_name']: wkbks_dir+item['template_file_name']},{ 'InventoryUpdateGuidance.pdf': wkbks_dir+'InventoryUpdateGuidance.pdf'}]
    recipientsActual = {'To':item['primary_data_coordinator'], 'bcc': 'jason.lally@sfgov.org'}
    if 'secondary_data_coordinator' in item.keys():
      recipientsActual['cc'] = item['secondary_data_coordinator']
    print recipientsActual
    recipientsFake =  { 'To': 'jason.lally@sfgov.org', 'cc':'janine.heiser@sfgov.org', 'bcc': 'jason.lally@sfgov.org'}
    mail_recipient = e.sendEmails(subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None, recipients=recipientsFake, attachment_dictList = attachment_dictList, isETL=False)

if __name__ == "__main__":
    main()
