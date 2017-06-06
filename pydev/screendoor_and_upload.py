
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


def mapKeys(columns):
  cols = [ col.lower().replace("-", '').replace(" ", "_").replace("__", '_').replace("(", '').replace(")", '') for col in columns]
  return dict(zip(columns, cols))

def getRequiredFieldCountInventory(row):
  if row['remove_system_not_in_use_or_custodianship_not_with_department'] == 'Yes':
    return 0
  elif row['exempt_from_public_systems_inventory'] == "Yes":
    return 4
  elif ( (row['exempt_from_public_systems_inventory'] == 'No') or (row['exempt_from_public_systems_inventory'] == '')):
    return 10
  return 0

def getRequiredFieldsCompleteInventory(row):
  rowKeys = row.keys()
  except_from_psi = ['exempt_from_public_systems_inventory', 'exemption_reason', 'department_custodian' , 'department_name_of_data_system']
  others = ['department_custodian', 'department_name_of_data_system' , 'remove_system_not_in_use_or_custodianship_not_with_department', 'exempt_from_public_systems_inventory','purpose','general_description_of_categories_or_types_of_data', 'vendor','product', 'frequency_data_collected', 'frequency_data_updated']
  if row['remove_system_not_in_use_or_custodianship_not_with_department'] == 'Yes':
    return 0
  elif row['exempt_from_public_systems_inventory'] == 'Yes':
    return sum([ 1 for key in rowKeys if row[key] != '' and key in except_from_psi])
  elif ((row['exempt_from_public_systems_inventory'] == 'No') or (row['exempt_from_public_systems_inventory'] == '')):
    return sum([ 1 for key in rowKeys if ((row[key] != '') and (key in others))])
  return 0

def getRequiredFieldCountDatasets(row):
  if row['revised_priority'] == 'Remove':
    return 0
  return 5

def getRequiredFieldsCompleteDatasets(row):
  rowKeys = row.keys()
  fields_to_cnt = ['department_or_division', 'inventory_name', 'inventory_description', 'value', 'data_classification']
  if row['revised_priority'] == 'Remove':
    return 0
  return sum([ 1 for key in rowKeys if ((row[key] != '') and (key in fields_to_cnt))])

def getCountsDepts(sQobj,base_url, fbf, key, results_key):
  if key == 'department_or_division':
    qry = '''%s%s.json?$query=SELECT %s, count(*) as %s GROUP BY %s ''' %(base_url, fbf, key, results_key, key)
  else:
    qry = '''%s%s.json?$query=SELECT %s as department_or_division, count(*) as %s GROUP BY %s ''' %(base_url, fbf, key, results_key, key)
  return sQobj.getQryFull(qry)


def getSubmittedCnt(sQobj,base_url, fbf, key, results_key, dept):
  qry = '''%s%s.json?$query=SELECT %s as department_or_division, count(*) as value WHERE %s = '%s' GROUP BY %s |> SELECT value ''' %(base_url, fbf, key, key, dept, key)
  return DatasetUtils.getResults(sQobj, qry)

def getSums(sQobj,base_url, fbf, key, dept_key,  dept):
  qry = '''%s%s.json?$query=SELECT SUM(%s) as value WHERE %s =  '%s' ''' % (base_url, fbf, key, dept_key, dept)
  return DatasetUtils.getResults(sQobj, qry)



def main():
  fieldConfigFile, config_inputdir, jobType =  parse_opts()
  configItems = ConfigUtils.setConfigs(config_inputdir, fieldConfigFile )
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  screendoor_stuff = ScreenDoorStuff(configItems)
  sQobj = SocrataQueries(clientItems, configItems, logger)

  qryColsSubmitted = 'department_or_division WHERE submitted = True'
  #submitted =  DatasetUtils.getDatasetAsDictListPageThrough(configItems['dd']['index']['fbf'], sQobj, qryColsSubmitted)
  #submitted = [ item['department_or_division'] for item in submitted ]
  submitted = []
  download_dir = screendoor_stuff._wkbk_uploads_dir


  ''''download the files from screendoor'''
  remove_files = FileUtils.remove_files_on_regex(screendoor_stuff._screendoor_configs['wkbk_uploads_dir'], "*.xlsx")
  downloadFiles = []
  datasets_to_load = {'Systems Inventory': [], 'Dataset Inventory': []}
  #
  #grab the most recent submission
  maxResponses = {}
  dept_key =  str(screendoor_stuff._screendoor_configs['keys_to_keep']['department']).strip()
  for response in screendoor_stuff._responses:
    if str(response['responses'][dept_key]).strip() in submitted:
      continue
    elif str(response['responses'][dept_key]).strip() in maxResponses.keys():
      '''need to find the actual time duration here'''
      #print "original"
      #print maxResponses[ response['responses'][dept_key]]
      #print "new"
      #print response['submitted_at']
      if DateUtils.compare_two_timestamps(response['submitted_at'], maxResponses[ response['responses'][dept_key]]):
        #print "we got a later submission"
        #print response['submitted_at']
        maxResponses[ response['responses'][dept_key]] = response['submitted_at']
    else:
      maxResponses[ response['responses'][dept_key]] = response['submitted_at']
  for response in screendoor_stuff._responses:
    dept = str(response['responses'][dept_key]).strip()
    if (response['submitted_at'] == maxResponses[dept]) and (not(dept in submitted )):
      file_info =  response['responses'][screendoor_stuff._screendoor_configs['keys_to_keep']['file']][0]
      file_info['submitted_at'] = response['submitted_at']
      file_info['dept'] = dept
      downloadFiles.append(file_info)
  #download the files and parse the workbks
  shts_to_keep = configItems['wkbks']['shts_to_keep']
  print shts_to_keep
  for fn in downloadFiles:
    print 
    print fn
    print
    downloaded = screendoor_stuff.getAttachment(fn['id'], fn['filename'])
    if downloaded:
      wkbk_stuff = WkbkUtils.get_shts(download_dir + fn['filename'])
      sht_names = [ sht for sht in wkbk_stuff['shts'] if sht in shts_to_keep]
      for sht in sht_names:
        df = WkbkUtils.getShtDf(wkbk_stuff, sht)
        key_dict = mapKeys(list(df.columns))
        df = PandasUtils.renameCols(df,key_dict)
        df =  PandasUtils.fillNaWithBlank(df)
        if sht == 'Systems Inventory':
          df['required_fields_count'] =  df.apply(lambda row: getRequiredFieldCountInventory(row), axis=1)
          df['required_fields_complete'] =  df.apply(lambda row:getRequiredFieldsCompleteInventory(row), axis=1)
          df['department_custodian'] = df['department_custodian'].astype(str)
          print df['department_custodian']

          df = df[df['department_custodian'] != '' ].reset_index()
        elif sht == 'Dataset Inventory':
          df['required_fields_count'] =  df.apply(lambda row: getRequiredFieldCountDatasets(row), axis=1)
          df['required_fields_complete'] =  df.apply(lambda row:getRequiredFieldsCompleteDatasets(row), axis=1)
          df['department_or_division'] = df['department_or_division'].astype(str)
          #print df['department_or_division']
          df = df[df['department_or_division'] != ''].reset_index()
          df['start_date'] =  df['start_date'].astype(str)
          df['end_date'] =  df['end_date'].astype(str)
        dfList = PandasUtils.convertDfToDictrows(df)
        dfList = [DictUtils.filterDictOnBlanks(DictUtils.filterDictOnNans(item)) for item in dfList]
        datasets_to_load[sht] = datasets_to_load[sht] + dfList

  #Upload the data to Socrata
  dataset_results = []
  for dataset in datasets_to_load.keys():
    dataset_info = {'Socrata Dataset Name': configItems['dd'][dataset]['dataset_name'], 'SrcRecordsCnt':len(datasets_to_load[dataset]), 'DatasetRecordsCnt':0, 'fourXFour': (configItems['dd'][dataset]['fbf']), 'row_id': ''}
    dataset_info = scrud.postDataToSocrata(dataset_info, datasets_to_load[dataset] )
    dataset_results.append(dataset_info)

  base_url = configItems['base_url_qry']
  fbf_datasets_inventory = configItems['dd']['Dataset Inventory']['fbf']
  fbf_systems_inventory = configItems['dd']['Systems Inventory']['fbf']
  results = getCountsDepts(sQobj,base_url, fbf_datasets_inventory, 'department_or_division', 'submitted_dataset_row_count')

  if results:
    for result in results:
      result['submitted'] = True
      result['submitted_systems_row_count'] = int(getSubmittedCnt(sQobj,base_url, fbf_systems_inventory, 'department_custodian', 'submitted_systems_row_count', result['department_or_division']))
      result['systems_required_total'] = int(getSums(sQobj,base_url, fbf_systems_inventory, 'required_fields_count', 'department_custodian', result['department_or_division'] ))
      result['systems_required_complete'] =  int(getSums(sQobj,base_url, fbf_systems_inventory, 'required_fields_complete', 'department_custodian', result['department_or_division'] ))
      result['systems_required_remaining'] = result['systems_required_total'] - result['systems_required_complete']
      result['datasets_required_total'] = int(getSums(sQobj,base_url, fbf_datasets_inventory , 'required_fields_count', 'department_or_division', result['department_or_division'] ))
      result['datasets_required_complete'] =  int(getSums(sQobj,base_url, fbf_datasets_inventory , 'required_fields_complete', 'department_or_division', result['department_or_division'] ))
      result['datasets_required_remaining'] = result['datasets_required_total'] - result['datasets_required_complete']
  dsse = JobStatusEmailerComposer(configItems, logger, jobType)
  dataset_info = {'Socrata Dataset Name': configItems['dd']['index']['dataset_name'], 'SrcRecordsCnt':len(results), 'DatasetRecordsCnt':0, 'fourXFour': configItems['dd']['index']['fbf'], 'row_id': 'department_or_division'}
  dataset_info = scrud.postDataToSocrata(dataset_info, results )
  dataset_info['isLoaded'] = 'success'
  dataset_results.append(dataset_info)
  dsse.sendJobStatusEmail(dataset_results)
  print dataset_results

#change subject line on ETL email


if __name__ == "__main__":
    main()
