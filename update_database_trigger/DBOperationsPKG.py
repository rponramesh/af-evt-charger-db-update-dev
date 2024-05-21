
import requests
import os
import sys
import json
import pandas as pd
import io
import psycopg2
import csv
import logging as log
from dotenv import load_dotenv
import tempfile


load_dotenv()

local_path = tempfile.gettempdir()


#------------------------------------------------------------------------#
#Note: 
#------------------------------------------------------------------------#
#These tables have static data that is not updated for each transaction. 
#Its not necessary to add this data to the database
# public.charger_capability_set
# public.charger_make_model
# public.charger_manufacturer_model
# public.connectors
# public.locations

#--connectorIds
# lastTelemetry -- deprecated
#  lastSession -- deprecated

#ignore deprecated tables 'lastSession.csv', 'lastTelemetry.csv'
#'lastSession.csv', == lastTransactions
#'lastTelemetry.csv' == lastMeterValues
#------------------------------------------------------------------------#



# mapping of tables in database to the csv data sources
db_tables_csv_files = {
    'charger_capability_set':'chargerCapabilitySet.csv',           #static data
    'charger_data':'chargerData.csv', 
    'charger_make_model':'chargerMakeModel.csv',                   #static data
    'charger_manufacturer_model':'chargerManufacturerModel.csv',   #static data
    'connector_status':'connectorStatus.csv',
    'connector_status_counts':'connectorSimplifiedStatusCounts.csv', 
    'connector_utilization':'connectorUtilization.csv', 
    'connectors':'connectors.csv',                                 #static data
    'last_meter_values':'lastMeterValues.csv',
    'latest_charger_status':'latestChargerStatus.csv',
    'locations':'location.csv',                                    #static data
    'insights':'insights.csv', 
    'transactions':'lastTransactions.csv',
    'transactions_final': 'transactionData_Final.csv'
}  


# mapping of table to insert queries
db_tables_query_str = {
    'charger_capability_set':"""COPY public.charger_capability_set(id, version, "values", "ocppProtocol", "chargingProfilesAnySupport", "chargingProfilesSupportsDefaultTxProfile", "chargingProfilesSupportsChargepointMax", "chargingProfilesSupportsTxProfile", "chargingProfilesStackLevelsSupported", "chargingProfilesMaxStackLevels", "chargingProfilesMaxSimultaneouslySupported", "chargingProfilesMaxSchedulePeriods", "chargingProfilesZeroPowerSupported", "chargingProfilesAcceptsAmps", "chargingProfilesAcceptsWatts", "chargingProfilesSupportsDischarge", "chargingProfilesMaxDischargePower", "remoteStartSupport", "remoteStopSupport", "sendsTransactionStart", "sendsTransactionStop", "sendsPlugInEvent", "sendsPlugOutEvent", "connectorStatusNotifSupport", "meterValuesAnySupport", "meterValuesContextStart", "meterValuesContextPeriodic", "meterValuesContextStop", measurands, "hardRebootSupported", "softRebootSupported", "configKeysGetAllSupported", "configKeysGetSpecificSupported", "configKeysSetSupported", "chargerId")""", 
    'charger_data':"""COPY public.charger_data("chargerId", "chargerName", "endpointAddress", "ocppProtocol", "registrationStatus", "chargePointYear", "chargePointVendor", "chargePointModel", "chargePointSerialNumber", "chargeBoxSerialNumber", "firmwareVersion", "firmwareUpdateStatus", "firmwareUpdateTimestamp", iccid, imsi, "meterType", "meterSerialNumber", "diagnosticsStatus", "diagnosticsTimestamp", "lastHeartbeatTimestamp", "lastHeardTimestamp", description, note, "autoRegisterTags", "groupId", "maxPower", "maxCurrent", voltage, "currentType", "depotId", "depotName", "fleetId", "externalId", latitude, longitude, created, updated, "connectorIds", "activeConnectorIds", "v2gEnabled", "livePowerImport", "livePowerImportKw", "lastKnownPowerImportKw", "livePowerImportUnit", "livePowerExport", "livePowerExportKw", "lastKnownPowerExportKw", "livePowerExportUnit", "powerUnit", "commissioningDate", "networkConnectionMethod", "networkConnectionDetails", "utilityLocationId", "isAdaAccessible", "evseInstallationType", "preferredProfileNumPhases", "importKwhLast24h")""", 
    'charger_make_model':"""COPY public.charger_make_model(id, "vendorName", "chargerModel", "modelHelperText", "currentType", "knownPowerRatingsKw", "knownOperatingCurrentsAmps", "knownOperatingVoltages", "chargerId")""",
    'charger_manufacturer_model':"""copy public.charger_manufacturer_model(id, name, "chargerId") """, 
    'connector_status':"""COPY public.connector_status("connectorStatus", "chargerId", "chargerNm", "connectorId", status, "currentCombinedStatus", "legacyStatus", "operationalStatus", "occupiedStatus", "simplifiedStatus", "errorCode", "errorInfo", "vendorId", "vendorErrorCode", "statusTimestamp")""",
    'connector_status_counts':"""COPY public.connector_status_counts("numConnectorsFaulted", "numConnectorsUnavailable", "numConnectorsPreparingOrCharging", "numConnectorsFinishedCharging", "numConnectorsUnoccupied", "chargerId")""", 
    'connector_utilization':"""COPY public.connector_utilization("connectorUtilization", "lastKnownPowerImportKw", "livePowerImportKw", "lastKnownPowerExportKw", "livePowerExportKw", "chargerId")""", 
    'connectors':"""COPY public.connectors(connectors, "connectorId", "chargerId", "connectorType", "maxPower", voltage, "currentType", "powerFactor")""", 
    'last_meter_values':"""COPY public.last_meter_values("lastMeterValues", "currentImportAmps", "currentImportAmpsTimestamp", voltage, "voltageTimestamp", "currentOfferedAmps", "currentOfferedAmpsTimestamp", "energyActiveImportRegisterKwh", "energyActiveImportRegisterKwhTimestamp", "powerActiveImportKw", "powerActiveImportKwTimestamp", "powerOfferedKw", "powerOfferedKwTimestamp", "powerActiveExportKw", "powerActiveExportKwTimestamp", "powerFactor", "powerFactorTimestamp", temperature, "temperatureTimestamp", soc, "socTimestamp", "currentExportAmps", "currentExportAmpsTimestamp", "energyActiveExportRegisterKwh", "energyActiveExportRegisterKwhTimestamp", "chargerId")""",
    'latest_charger_status':"""COPY public.latest_charger_status(status, "currentCombinedStatus", "statusTimestamp", "operationalStatus", "connectivityStatus", "chargerId")""",
    'locations':"""COPY public.locations(type, coordinates, "chargerId")""", 
    'insights':"""COPY public.insights("chargerHealth", "chargerId")""", 
    'transactions':"""COPY public.transactions("lastTransactions", id, "transactionPk", "connectorId", "chargerPk", "chargerId", "chargerName", "depotId", "organizationId", "ocppTag", "stopReason", "stopEventActor", "startTimestamp", "stopTimestamp", "updatedTimestamp", "totalEnergyImported", "totalEnergyExported", "meanPowerImport", "maxPowerImport", "latestPowerImport", "meanPowerExport", "maxPowerExport", "latestPowerExport", "meanPowerOffered", "maxPowerOffered", "latestPowerOffered", "meanAmperageImport", "maxAmperageImport", "latestAmperageImport", "meanAmperageOffered", "maxAmperageOffered", "latestAmperageOffered", "minVoltage", "meanVoltage", "maxVoltage", "latestVoltage", "minFrequency", "meanFrequency", "maxFrequency", "latestFrequency", "startingTemperature", "minTemperature", "meanTemperature", "maxTemperature", "latestTemperature", "startingSoc", "latestSoc", timezone)""",
    'transactions_final':"""COPY public.transactions_final( id, "transactionPk", "connectorId", "chargerPk", "chargerId", "chargerName", "depotId", "organizationId", "ocppTag", "stopReason", "stopEventActor", "startTimestamp", "stopTimestamp", "updatedTimestamp", "totalEnergyImported", "totalEnergyExported", "meanPowerImport", "maxPowerImport", "latestPowerImport", "meanPowerExport", "maxPowerExport", "latestPowerExport", "meanPowerOffered", "maxPowerOffered", "latestPowerOffered", "meanAmperageImport", "maxAmperageImport", "latestAmperageImport", "meanAmperageOffered", "maxAmperageOffered", "latestAmperageOffered", "minVoltage", "meanVoltage", "maxVoltage", "latestVoltage", "minFrequency", "meanFrequency", "maxFrequency", "latestFrequency", "startingTemperature", "minTemperature", "meanTemperature", "maxTemperature", "latestTemperature", "startingSoc", "latestSoc", timezone)"""  

}


# update table
""" UPDATE public.transactions_final
       SET "stopReason" = %s, "stopEventActor" = %s, "startTimestamp" = %s, "stopTimestamp" = %s, "updatedTimestamp" = %s, "totalEnergyImported" = %s 
     WHERE "transactionPk" = %s"""
  
tablePrefix_ = 'public.'

class DBOperations:

    def __init__(self) -> None:
        pass


    def connectToDB(self):
    
        db_host = os.environ.get('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_pwd = os.getenv('DB_PWD')
        
        # db_host = "10.135.2.17"
        # db_name = "cae_sandbox_db"
        # db_user = "vmadmin@azruse2caeevt1"
        # db_pwd = "qi6mgS@!VL7m"

        log.info(f"db_host: {db_host},  /db_name: {db_name}, /db_user: {db_user}, /db_pwd: {db_pwd}")
        #conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pwd)
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_pwd)
        
        return conn
    
    def printTables(self):
        for k,v in db_tables_csv_files.items():
            tableName_ = tablePrefix_ + k
            fileName_ = v

            fileName_=os.path.join(local_path, fileName_)


            print('updating: ',tableName_, ' from file: ',fileName_)


    def pushCSVToPostgreSQL(self):
        print("pushing to DB")


        print("pushCSVToPostgreSQL")
        conn = self.connectToDB()
        print("connected to db")
        # create a cursor
        cursor = conn.cursor()
        print("connected to cursor")

        for k,v in db_tables_csv_files.items():
            tableName_ = tablePrefix_ + k
            fileName_ = v
            fileName_=os.path.join(local_path, fileName_)

            print('updating: ',tableName_, ' from file: ',fileName_)

            try:
                if(os.path.isfile(fileName_)):
                    with open(fileName_, 'r') as f:
                        #cursor.copy_expert(f"COPY <database>.<schema>.<table> FROM STDIN (FORMAT 'csv', HEADER false)" , buffer)
                        #cursor.copy_expert(f"COPY "+tableName_+" FROM STDIN (FORMAT 'csv', HEADER true)" , f)
                        #cursor.copy_expert(f"COPY public.connector_status_counts FROM STDIN (FORMAT 'csv', HEADER true)" , f)
                        #-------------------------#
                        query_str = db_tables_query_str.get(k)
                        copy_sql = query_str + """
                            from stdin with
                                csv
                                header
                                delimiter as ','
                            """
                        cursor.copy_expert(sql=copy_sql, file=f)
                        #-------------------------#
                else:
                    log.info(str('File: '+ fileName_+ ' does not exist.'))
            except (Exception, psycopg2.DatabaseError) as error:
                log.error(str("Error: %s" % error))
                continue

        conn.commit()
        conn.close()


    #Sample code to read from a PostgreSQL table
    def readFromTable(self):

        conn = self.connectToDB()
        # create a cursor
        cursor = conn.cursor()

        postgreSQL_select_Query = "select * from charger_manufacturer_model"

        cursor.execute(postgreSQL_select_Query)
        column_names = [desc[0] for desc in cursor.description]
        log.info(str("Column names: {}\n".format(column_names)))

        result = cursor.fetchone()

        log.info(result)
        log.info("Selecting rows from mobile table using cursor.fetchall")  
        mobile_records = cursor.fetchall()

        log.info("Print each row and it's columns values")
        for row in mobile_records:
            log.info(str("Id = "+ row[0] ))
            log.info(str("Model = "+ row[1]))
            log.info(str("Price  = "+ row[2]+ "\n"))


    # use for specific one off cases 
    # A different syntax for inserting data into table
    def insertIntoTable(self, fileName_, tableName_, query_str):

        conn = self.connectToDB()
        # create a cursor
        cursor = conn.cursor()

        filePath_ = os.path.join(local_path, fileName_)
        
        log.info(str('filePath_'+ filePath_))

        try:
            if(os.path.isfile(fileName_)):
                #with open(fileName_, 'r', encoding='utf-8') as f:
                with open(filePath_, 'r', encoding='utf-8') as f:
                    
                    #table_col_str = """copy public.charger_manufacturer_model(id, name, "chargerId") """
                    copy_sql = query_str + """
                        from stdin with
                            csv
                            header
                            delimiter as ','
                        """
                    
                    #with open(filePath_, 'r') as f:
                    #version 1 -- works well
                    cursor.copy_expert(sql=copy_sql, file=f)
            else:
                log.info(str('File: '+ fileName_+ ' does not exist.'))
        except (Exception, psycopg2.DatabaseError) as error:
            log.error(str("Error: %s" % error))
            pass

        conn.commit()
        conn.close()
    

    #----------------------------------------------#   
    def testReadfromDB(self):

        log.info('..start testReadfromDB')
        conn = self.connectToDB()
        # create a cursor
        cursor = conn.cursor()
        
        cursor.execute("""select * from public.locations limit 1000;""")
        
        row_1 = cursor.fetchone()
        log.info('row_1:  ')
        log.info(row_1)
        
        #rows = cursor.fetchall()
        # Print all rows
        #for row in rows:
        #    print("Data row = (%s, %s, %s, %s)" %(str(row[0]), str(row[1]), str(row[2]), str(row[3])))
        cursor.close()
        conn.close()
        
        log.info('....end testReadfromDB')


    def testInsertIntoDB(self):

        log.info('..start testInsertIntoDB')
        conn = self.connectToDB()
        # create a cursor
        cursor = conn.cursor()
        
        cursor.execute("""INSERT INTO public.locations(type, coordinates, "chargerId") VALUES (%s, %s, %s);""", ("LINE", "150", "99"))
            
        conn.commit()
        cursor.close()
        conn.close()
        log.info('..end testInsertIntoDB')


    def testAccessToDBOperationsPKG(self) -> None:
        print('Access to DBOperations PKG OK...')
        log.info('Access to DBOperations PKG OK...log...')


    def testConnectionToPostgreSQL(self):
        log.info("pushCSVToPostgreSQL......")
        conn = self.connectToDB()
        log.info("connected to db")
        # create a cursor
        cursor = conn.cursor()
        log.info("connected to cursor")
        cursor.close()
        conn.close()