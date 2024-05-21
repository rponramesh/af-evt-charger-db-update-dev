import requests #API calls
import pandas as pd
import pytz
import logging as log
import os
import sys
import json
import io
import datetime
import glob      #search for csv files
import shutil    #files/folder operations
import re        #regular expression
import csv
from . import DBOperationsPKG
import tempfile
import timezonefinder

local_path = tempfile.gettempdir()

from enum import Enum
class CsmsType(Enum):
    SYNOP = 1
    EPIC = 2


def testAccessToprocessAPIData():
    print('Access to processAPIData OK...')
    #dbOpsObj = DBOperations()
    #dbOpsObj.testAccessToDBOperationsPKG()


def getDataFromURL(url,csmsType,api_key):
    log.debug('..getDataFromURL..')

    #api_key = os.getenv('API_KEY')

    api_key = api_key

    log.debug("csmsType "+str(csmsType))
    log.debug(str("api_key: "+api_key))
    
    headers = {
            'Content-type': 'application/json',
        }
    
    if(CsmsType.SYNOP==csmsType):
        headers['x-api-key']=api_key
    elif(CsmsType.EPIC==csmsType):
        headers['Token-Authorization']=api_key

    
    # Authorization
    # sending get request and saving the response as response object
    response = requests.get(url, headers=headers)

    if response.status_code < 400:
        log.info('Response is ok!')
    else:
        log.info("Response isn't ok!")

    # return data to be extracted in json format
    retVal = response

    return retVal

  
def normalize_json(data: dict) -> dict:
  
    new_data = dict()
    for key, value in data.items():
        if not isinstance(value, dict):
            new_data[key] = value
        else:
            for k, v in value.items():
                new_data[key + "_" + k] = v
      
    return new_data



def appendToJson(inputs, outFile):
    df = pd.json_normalize(inputs)
    df.to_csv(outFile, mode='a', index=False, header=False)


def parseList(value_):
    if isinstance(value_, list): # if its an array, append with ';' character
        #print('list value: ', value_)
        if(value_):  # list is not empty
            y=''
            for x in value_:
                y = y + str(x) +'  '
                
            value_ = y.strip() #remove excess trailing space
        else: #use 9999 as value for empty list
            value_ = [9999]

    return value_

def saveDataToFile(filename_, data_dict_2, commonKeys):

    if(('chargerId' not in data_dict_2.keys()) and (commonKeys )) :
        log.debug('chargerId not in data_dict_split')
        if('chargerId' in commonKeys):
            #data_dict_2.update(commonKeys)
            data_dict_2['chargerId'] = commonKeys['chargerId']
            log.debug('chargerId in commonKeys')

    #Add chargerID and charger Name if its missing
    if(filename_ == "connectorStatus"):
        if(data_dict_2['chargerId'] == None):
            data_dict_2['chargerId'] = commonKeys['chargerId']

        if(data_dict_2['chargerNm'] == None):
            data_dict_2['chargerNm'] = commonKeys['chargerNm']


    print ('****Writing file: ', filename_) 
    #print ('data_dict_2: ', data_dict_2)
    df_1 = pd.DataFrame(data_dict_2, index=[0])
    #save current dataframe to a new csv file
    #df_1.to_csv(filename_+".csv", index=False)

    file_path_1 = os.path.join(local_path, filename_+".csv")   
    if(os.path.isfile(file_path_1)): # if file exists append
        df_1.to_csv(file_path_1, mode='a', index=False, header=False)
    else:
        df_1.to_csv(file_path_1, index=False)


def parseJSONData(URLPrefix, chargerID,csmsType,api_key):

    log.debug('..parsing JSONData..')
    url_2 = URLPrefix+ chargerID
    ret_1 = getDataFromURL(url_2,csmsType,api_key)
    ret_1 = ret_1.json()

    inputVal = ret_1
    log.info("inputVal data")
    log.info(json.dumps(inputVal))

    #outFile = 'testData.csv'

    df = pd.json_normalize(inputVal)
    #df.to_csv(outFile) #"output.csv"
    #df.to_csv(outFile, mode='a', index=False, header=False)

    #inputs = inputVal["location"]["coordinates"]
    #print("inputs: ", inputs)
    #appendToJson(inputs, outFile)
    

    # Read the JSON file as python dictionary
    # data = read_json(filename="article.json")
    # Normalize the nested python dict 
    new_data = normalize_json(data=inputVal)
    log.debug("New dict.....:\n")
    log.debug(new_data)

    log.debug(json.dumps(new_data))

    #remove deprecated keys from the dictionary
    new_data = removeKeysFromDict(new_data)

    #clean up the dictionary by removing deprecated entries
    # 'lastTelemetry': None, 'lastSession
    df2 = pd.DataFrame.from_dict(new_data, orient="index")
    print(df2,"df2 printed")
    
    df_dict = df2.to_dict()


    log.info("FULL")
    log.info(str(json.dumps(df_dict)))

    #df2.to_csv("TestData_2.csv", mode='a', index=False, header=False)
    data_dict_main = {}
    data_dict_split = {}
    #print("keys_dict: ", df2.keys())
    #assuming only one level of nesting and all data is in sorted order
    prefix_ = ""
    filename_ = ""
    writeFile = False
    prevKey_ = ''
    index = 0
    # to store charger ID and any other foreign keys
    commonKeys = {}

    for key, row in df2.iterrows():
        #print("key: ", key, " row: ", row)
        index +=1
        value_ =  row[0]

        #save chargerNm key for connector status
        if(key == 'chargerName'):  #"chargerName": "Garage_07",
            commonKeys["chargerNm"] = value_

        if isinstance(value_, list):
            value_ = parseList(value_)
            #print ('value is list')
            
        newKey_ = ''
        if("_" in key):
            log.debug ("_ in key")
            prefix_ = key.split('_')[0]   #use first part of key this is a file name
            newKey_ = key.split('_')[1] 
            log.debug(str("prefix_: "+ prefix_+ " filename_ : "+filename_ + " Key_: "+ key+ " newKey_: "+ newKey_))
            if filename_ == "":
                filename_ = prefix_
                log.info("filename_ == null")
                
            if prevKey_ == "":
                prevKey_ = newKey_
                log.debug("prevKey_ == null")
           
            #print(".2.prefix_: ", prefix_, " filename_ : ",filename_ , " Key_: ", key, " newKey_: ", newKey_, " prevKey_: ", prevKey_)
            #print('isinstance(value_, dict):: ',isinstance(value_, dict))
            
            #Write data to file if these conditions are met
            #new prefix is detected, suffix changed from _0 and _1, and is numeric and its a dict or its end of file
            if(prefix_ != filename_ or  
               ((prevKey_ != newKey_) & (newKey_.isnumeric()) & isinstance(value_, dict)) or
               (index == len(df2)) ): # cover _0 and _1 cases
                
                writeFile = True
                #write the last value to dict - may not be needed as write file is repeated at the 
                if((index == len(df2))):
                    data_dict_split[newKey_] = value_
                log.info("data_dict_split")
                log.info(str(json.dumps(data_dict_split)))
                saveDataToFile(filename_, data_dict_split, commonKeys)
                
                #update the filename
                filename_ = prefix_
                print("...start new file: ", filename_)
                data_dict_split.clear()

                # only add if the new value is not str as in chargerMakeModel_chargerMakeModelKnownOperatingParameters : {}
                if(newKey_.isnumeric()): 
                    data_dict_split[prefix_] = newKey_
                    #print("..Numeric...newKey_: ", newKey_, '/prefix_: ',prefix_)
                else:
                    data_dict_split[newKey_] = value_
                    #print("...newKey_: ", newKey_, '/value_: ',value_)

            #reading a key with the same prefix
            value_ = parseList(value_) # if value is a list, convert to a delimited string

            if not isinstance(value_, dict):
                data_dict_split[newKey_] = value_
            else: # its a dictionary
                # add an additional row for case 'connectorStatus_0', 'connectorStatus_1' etc
                #print('..type(newKey_)', type(newKey_), '/',newKey_.isnumeric())
                if(newKey_.isnumeric()): # only add if the new value is not str as in chargerMakeModel_chargerMakeModelKnownOperatingParameters : {}
                    data_dict_split[prefix_] = newKey_ # connectorStatus=0
                    #print('...is NUM..if(type(newKey_) != str): prefix_: ', prefix_, '//newKey_=',newKey_)
                for k2, v2 in value_.items():
                    v2 = parseList(v2) #check if value is a list
                    data_dict_split[k2] = v2
    
        else:
            data_dict_main[key] = value_ #row[0] #row.values

            #Save chargerId and chargerName in a dict to replace null values in any files later
            if(key == 'chargerId'):
                commonKeys[key] = value_

        prevKey_ = newKey_
        
    #write remaining items in data_dict_split to a file # ChargerData.csv
    if(len(data_dict_split) > 0):
        saveDataToFile(filename_, data_dict_split, commonKeys)
    
    #print(' index: ', index)
    #print('final data_dict_split == ', data_dict_split)
    #print('data_dict_main == ', data_dict_main)
    df_2 = pd.DataFrame(data_dict_main, index=[0])
    #save dataframe to csv file
    chargerData_filePath = os.path.join(local_path, "ChargerData.csv")

    df_2.to_csv(chargerData_filePath, index=False)



def backUpCSVFiles(chargerID):
    CSVFolder = os.path.join(local_path, "CSVFiles")

    if(not os.path.exists(CSVFolder)):
        os.mkdir(CSVFolder)

    timeStamp = str(datetime.datetime.now()).replace(':','-')
    ChargerFolder = os.path.join(CSVFolder, chargerID +"_"+ timeStamp)
    if(not os.path.exists(ChargerFolder)):
        os.mkdir(ChargerFolder)

    files = glob.glob(local_path+ "/*.csv")
    for filename in files:
        #print('source: ',filename)
        #print('destination: ',ChargerFolder)
        shutil.move(filename, ChargerFolder)

#remove lastTelemetry, lastSession keys from dictionary
def removeKeysFromDict(input_dict):

    temp_dict_1 =  {k:input_dict[k] for k in input_dict if re.match('^lastTelemetry', k)}  
    if(len(temp_dict_1) !=0):
        # Iterating through the keys
        for key in temp_dict_1.keys():
            del input_dict[key]
    
    temp_dict_2 =  {k:input_dict[k] for k in input_dict if re.match('^lastSession', k)}  
    if(len(temp_dict_2) !=0):
        # Iterating through the keys
        for key in temp_dict_2.keys():
            del input_dict[key]
    
    return input_dict


#Download Charger data from SynOp and upload to PostgreSQL database
def processChargerData(URLPrefix, chargersList,csmsType,api_key):

    print('...processing ChargerData...')
    for chargerID in chargersList:
        log.info(str("current chargerID: "+ chargerID))
        parseJSONData(URLPrefix, chargerID,csmsType,api_key)
                
        #Push data into PostgreSQL tables
        log.info('****************************************')
        log.info('..Uploading data to cae_sandbox_db database..')
        log.info('****************************************')
        
        dbOpsObj = DBOperationsPKG.DBOperations()
        dbOpsObj.pushCSVToPostgreSQL()

        #Delete csv files or move to new folder
        #create a folder - CSVFiles/WBAT42BAAA+timestamp
        #create folder with chargerID and timestamp
        backUpCSVFiles(chargerID)
        
# completed transactions are processed seperately from charger data
def processTransactionData(csmsType,api_key): #fileSuffix

    trFilename_ = 'transactionData_Final'  
    outFile = trFilename_ + '.csv'
    
    #Timezone set to UTC using pytz; default EST
    #startTimeFrom = "2023-10-09T00%3A00%3A00.000Z" #09-01 to 09-24   #07-01 to 08-31
    #10 min time delta
    td = datetime.timedelta(minutes=10)
    startTimeFrom    = datetime.datetime.now(pytz.utc)
    startTimeFrom = (startTimeFrom - td).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    stopTimeTo    = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z") 
    #stopTimeTo = stopTimeTo.replace(':', '%3A')

    suffix_from = "2023-10-09"
    suffix_to = datetime.datetime.now().strftime("%Y-%m-%dT%H_%M_%S") # 2023-10-06T16_11_51.csv
    fileSuffix_ = suffix_from + '_'+ suffix_to  #"2023-09-21_2023-10-06_3"
    print('startTimeFrom: ', startTimeFrom, ' stopTimeTo: ', stopTimeTo)
   
    
    URLPrefix = "https://api.prod.synop.ai/api/transactions?"
    URLFinal = URLPrefix + "startTimeFrom="+startTimeFrom+"&stopTimeTo="+stopTimeTo+"&count=1000"
    #print("URLFinal: ", URLFinal)

    inputData = getDataFromURL(URLFinal,csmsType,api_key)
    #print("inputData=",inputData)
    inputData = inputData.json()

    items = inputData['items']
    print("items=",items)
    #outFile = 'transactionData_' + fileSuffix+ '.csv'

    df = pd.json_normalize(items)
    #print(df)
    outFile_path_1 = os.path.join(local_path, outFile)   
    if(os.path.isfile(outFile_path_1)): # if file exists append
        df.to_csv(outFile_path_1, mode='a', index=False, header=False)
    else:
        df.to_csv(outFile_path_1, index=False)
    

    #df.to_csv(outFile, index=False) # dont write the index to file
    #ignore index when reading - df.read_csv(filename, index_col=False)


    #Push data into PostgreSQL tables
    log.info('****************************************')
    log.info('..Uploading transaction data to cae_sandbox_db database..')
    log.info('****************************************')
    
    dbOpsObj = DBOperationsPKG.DBOperations()
    dbOpsObj.pushCSVToPostgreSQL()

    # create a CSVFiles folder with chargerID and timestamp - CSVFiles/WBAT42BAAA+timestamp
    csvFOlderName = trFilename_ + datetime.datetime.now().strftime("%Y-%m-%d")
    backUpCSVFiles(csvFOlderName)
    

def UpdateDBdriver(): 
    #log.setLevel(log.DEBUG)
    synopApi=os.getenv("SYNOP_API_KEY")
    log.basicConfig(filename='output.log',format='%(levelname)s:%(message)s', level=log.DEBUG) #log.DEBUG
    log.info('...Processinging Charger data...')

    ##"chargerId"
    chargersList = [ "WBAT42A015", "WBAT42A020",  "WBAT42A047","WBAT42A051", "WBAT42A053", "WBAT42A071",  "WBAT42A089", 
    "WBAT42B011","WBAT42B044", "WBAT42B064" ]

    additional_chargers=['WBAT43P377','WBAT43P291','WBAT43P401','WBAT43Q027','WBAT43G214','WBAT43P403','WBAT43P304','WBAT43P420','WBAT43P418','WBAT43P481']

    #chargersList = [   "WBAT42B064" ] #"WBAT42A015", #error - WBAT42A015 ; ok - WBAT42B064

    ChargerNames = {"WBAT42A015": "EngDev", "WBAT42A020": "Garage_08", "WBAT42A047": "Garage_04", "WBAT42A051": "Garage_02", 
                    "WBAT42A053": "Garage_05", "WBAT42A071": "Garage_01", "WBAT42A089": "Garage_09", "WBAT42B011": "Garage_03", 
                    "WBAT42B044": "Garage_06", "WBAT42B064": "Garage_07"}
    chargersList+=additional_chargers


    URLPrefix = "https://api.prod.synop.ai/api/chargers/"
    processChargerData(URLPrefix, chargersList,CsmsType.SYNOP,synopApi)

        
    #Transactions Final
    processTransactionData(CsmsType.SYNOP,synopApi)

def UpdateEpicData():

    log.info('... Updating Epic Data ...')

    URLPrefix = "https://borgwarner.epiccharging.com/api/external/v1/report"
    epicApi=os.getenv("EPIC_API_KEY")

    chargers=getAllChargers(URLPrefix,CsmsType.EPIC,epicApi)


    for charger in chargers:
        uuid=charger["id"]
        print(uuid)
    
    tf = timezonefinder.TimezoneFinder()

    # From the lat/long, get the tz-database-style time zone name (e.g. 'America/Vancouver') or None
    timezone_str = tf.certain_timezone_at(lat=49.2827, lng=-123.1207)





def getAllChargers(URLPrefix,csmsType,api_key):
    log.info('... Getting all Chargers Epic ...')

    apiUrl=URLPrefix+"/charger-status/"

    inputData = getDataFromURL(apiUrl,csmsType,api_key)
    
    inputData = inputData.json()


    if (not inputData):
        return []
    
    return inputData["results"]
