# coding: utf-8
#!/usr/bin/env python

import pandas as pd
import json
from pandas.io.json import json_normalize
import unicodedata as ucd

class PandasUtils:

  @staticmethod
  def getWkbk(fn):
    wkbk = pd.ExcelFile(fn)
    return wkbk

  @staticmethod
  def get_dataset_as_dfList(pickle_data_dir, json_file, base_url):
    json_obj = FileUtils.loadJsonFile(pickle_data_dir, json_file)
    df = PandasUtils.makeDfFromJson(json_obj)
    df_list = PandasUtils.convertDfToDictrows(df)
    return df_list

  @staticmethod
  def getDictListAsMappedDict(fieldColKey, fieldColValue, dictList=None):
    '''maps a dict list to a dict of k,v pairs'''
    dictMapped = {}
    if dictList:
      for item in dictList:
        dictMapped[ item[fieldColKey] ] = item[fieldColValue]
    return dictMapped


  @staticmethod
  def removeCols(df, list_of_cols_to_remove):
    '''removes cols inplace'''
    df_col = list(df.columns)
    #check to make sure that column exists in the dataframe
    list_of_cols_to_remove = [col for col in list_of_cols_to_remove if col in df_col]
    return df.drop(list_of_cols_to_remove, axis=1)


  @staticmethod
  def loadCsv(fullpath):
    df = None
    try:
      df = pd.read_csv(fullpath)
    except Exception, e:
      print str(e)
    return df

  @staticmethod
  def fillNaWithBlank(df):
    return df.fillna("")

  @staticmethod
  def makeDfFromJson(json_obj):
    df = json_normalize(json_obj)
    return df

  @staticmethod
  def getDFColumnTypes(df):
    '''returns a dict of the types of columns in a df'''
    g = df.columns.to_series().groupby(df.dtypes).groups
    return g

  @staticmethod
  def convertDfToDictrows(df):
    if (not(df is None)):
      return df.to_dict(orient='records')
    return []

  @staticmethod
  def mapFieldNames(df, field_mapping_dict):
    return df.rename(columns=field_mapping_dict)

  @staticmethod
  def renameCols(df, colMappingDict):
    df = df.rename(columns=colMappingDict)
    return df

  @staticmethod
  def groupbyCountStar(df, group_by_list):
    return df.groupby(group_by_list).size().reset_index(name='count')

  @staticmethod
  def getGrpByCountStarColumnMax(df, group_by_list, max_col):
    return  df.groupby(group_by_list, sort=False)[max_col].max().reset_index()
  
  @staticmethod
  def colToLower(df, field_name):
    '''strips off white space and converts the col to lower'''
    df[field_name] = df[field_name].astype(str)
    df[field_name] = df[field_name].str.lower()
    df[field_name] = df[field_name].map(str.strip)
    return df

  @staticmethod
  def makeLookupDictOnTwo(df, key_col, val_col):
      return dict(zip(df[key_col], df[val_col]))

class DatasetUtils:
  '''class to grab datasets off the open data portal'''
  @staticmethod
  def getDatasetAsDFPageThrough(fbf, sQobj, base_url, qryCols):
    '''grabs a dataset, returns a df'''
    df = None
    results =  sQobj.pageThroughResultsSelect(fbf, qryCols)
    if results:
      df = PandasUtils.makeDfFromJson(json_obj)
    return df

  @staticmethod
  def getDatasetAsDictListPageThrough(fbf, sQobj, qryCols):
    '''grabs a dataset, returns a df'''
    df = None
    results =  sQobj.pageThroughResultsSelect(fbf, qryCols)
    if results:
      df = PandasUtils.makeDfFromJson(results)
      df = PandasUtils.fillNaWithBlank(df)
    return PandasUtils.convertDfToDictrows(df)

  @staticmethod
  def getDatasetAsDfFullQry(sQobj, qry):
    '''pass a full qry, returns a df'''
    results = sQobj.getQryFull(qry)
    if results:
      return PandasUtils.makeDfFromJson(results)
    return None

  @staticmethod
  def resultsToDictListFullQry(sQobj, qry):
    '''pass a full qry, returns a dictList of Results'''
    results = sQobj.getQryFull(qry)
    if results:
      df = PandasUtils.makeDfFromJson(results)
      return PandasUtils.convertDfToDictrows(df)
    return None

  @staticmethod
  def getResults(sQobj, qry):
    '''gets results from portal'''
    results = sQobj.getQryFull(qry)
    if results:
      #if (not(type(results) is dict )):
      if (len(results) > 0) and 'value' in results[0].keys():
        try:
          return  int(results[0]['value'])
        except:
          try:
            return round(float(results[0]['value']),2)
          except:
            return results[0]['value']
      if (len(results) > 0) and 'cnt' in results[0].keys():
        return  int(results[0]['cnt'])
    return 0

if __name__ == "__main__":
    main()
