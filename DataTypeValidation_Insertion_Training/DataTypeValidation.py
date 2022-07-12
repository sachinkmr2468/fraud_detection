from application_logging.logger import App_Logger
from datetime import datetime
import cassandra
import os
import shutil
import pandas as pd
from os import listdir
import csv
from app_exception.exception import AppException
import sys

ob = App_Logger()

class dbOperation:
    """
      This class shall be used for handling all the cassandra operations.
      """

    def __init__(self):
        pass

    def databaseConnection(self):

        """
                Method Name: dataBaseConnection
                Description: This method set up connection with cassandra cloud
                Output: Connection to the DB
                On Failure: Raise ConnectionError
                """
        try:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider

            cloud_config = {
                'secure_connect_bundle': 'secure-connect-test1.zip'
            }
            auth_provider = PlainTextAuthProvider('pNlNfEEFSOrdfmlPYMgKqiYA','xfZZt0TZFX+aK6q.6kL8J4PJoiWg.9duB0_a1Mc9dGNR8ZMGI.DiKR4rkBYn4swPe+wg_vneHEb_4E9MjRMZwzIUvCp5StSoElZg,Y5.0k28ZXA7TwazXA_yAZ2SHYlw')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute("select release_version from system.local").one()
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            ob.log(file, 'connection was successful')
        except ConnectionError:
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        return session

    def createTableDb(self):
        """
                                Method Name: createTableDb
                                Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                                Output: None
                                On Failure: Raise Exception
                                """
        conn = self.databaseConnection()
        try: #need to define the column names and its datatype before running
            row = conn.execute(
                "CREATE TABLE IF NOT EXISTS test2.good_rawwa (months_as_customer int PRIMARY KEY, age int, policy_number int, policy_bind_date text, policy_state text, policy_csl text, policy_deductable int, policy_annual_premium int, umbrella_limit int, insured_zip int, insured_sex text, insured_education_level text, insured_occupation text, insured_hobbies text, insured_relationship text, capital_gains int, capital_loss int, incident_date text, incident_type text, collision_type text, incident_severity text, authorities_contacted text, incident_state text, incident_city text, incident_location text, incident_hour_of_the_day int, number_of_vehicles_involved int, property_damage text, bodily_injuries int, witnesses int, police_report_available text, total_claim_amount int, injury_claim int, property_claim int, vehicle_claim int, auto_make text, auto_model text, auto_year int, fraud_reported text) ;").one()
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Tables created successfully!!")
            file.close()
        except Exception as e:
            file = open('Training_Logs\DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while creating table: %s " % e)
            file.close()

    def insertIntoTableGoodData(self):
        """
                                       Method Name: insertIntoTableGoodData
                                       Description: This method inserts the Good data files from the Good_Raw folder into the
                                                    above created table.
                                       Output: None
                                       On Failure: Raise Exception
                """
        conn = self.databaseConnection()
        goodfilepath = 'Training_Raw_files_validated/Good_Raw/'
        try:
            for i in os.listdir(goodfilepath):
                with open(goodfilepath + '/' + i, 'r') as f:
                    next(f)
                    csv_reader = csv.reader(f)
                    for line in csv_reader:
                        conn.execute("insert into test2.good_rawwa (months_as_customer,age,policy_number,policy_bind_date,policy_state,policy_csl,policy_deductable,policy_annual_premium,umbrella_limit,insured_zip,insured_sex,insured_education_level,insured_occupation,insured_hobbies,insured_relationship,capital_gains,capital_loss,incident_date,incident_type,collision_type,incident_severity,authorities_contacted,incident_state,incident_city,incident_location,incident_hour_of_the_day,number_of_vehicles_involved,property_damage,bodily_injuries,witnesses,police_report_available,total_claim_amount,injury_claim,property_claim,vehicle_claim,auto_make,auto_model,auto_year,fraud_reported) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                     [int(line[0]),int(line[1]),int(line[2]),(line[4]),(line[5]),(line[6]),int(line[7]),int(line[8]),int(line[9]),int(line[10]),(line[11]),(line[12]),(line[13]),(line[14]),(line[15]),int(line[16]),int(line[17]),(line[18]),(line[19]),(line[20]),(line[21]),(line[22]),(line[23]),(line[24]),(line[25]),int(line[26]),int(line[27]),(line[28]),int(line[29]),int(line[30]),(line[31]),int(line[32]),int(line[33]),int(line[34]),int(line[35]),(line[36]),(line[37]),int(line[38]),(line[39])])
                    print('Finished')
                    file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
                    ob.log(file, "data loaded successfully!!")
                    file.close()
        except Exception as e:
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while creating table: %s " % e)
            file.close()

    def selectingDatafromtableintocsv(self):
        """
                                       Method Name: selectingDatafromtableintocsv
                                       Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                                    above created .
                                       Output: None
                                       On Failure: Raise Exception
                """
        conn = self.databaseConnection()
        try:
            self.fileFromDb = 'Training_FileFromDB/'
            self.fileName = 'InputFileaa.csv'
#            fileName = r'B:\project\pythonmyproject\Training_FileFromDB\'InputFiless.csv'
            field1 = ['months_as_customer','age','policy_number','policy_bind_date','policy_state','policy_csl','policy_deductable','policy_annual_premium','umbrella_limit','insured_zip','insured_sex','insured_education_level','insured_occupation','insured_hobbies','insured_relationship','capital_gains','capital_loss','incident_date','incident_type','collision_type','incident_severity','authorities_contacted','incident_state','incident_city','incident_location','incident_hour_of_the_day','number_of_vehicles_involved','property_damage','bodily_injuries','witnesses','police_report_available','total_claim_amount','injury_claim','property_claim','vehicle_claim','auto_make','auto_model','auto_year','fraud_reported']
            row = conn.execute("select * from test2.good_rawwa")
            with open(self.fileFromDb + self.fileName, 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(field1)
                csvwriter.writerows(row)
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "data loaded to csv successfully!!")
            file.close()
        except Exception as e:
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            ob.log(file, "Error while extracting data: %s " % e)
            file.close()