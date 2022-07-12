import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
import sys
from application_logging.logger import App_Logger
from app_exception.exception import AppException

dicts1 = dicts = {"SampleFileName": "fraudDetection_021119920_010222.csv",
"LengthOfDateStampInFile": 9,
"LengthOfTimeStampInFile": 6,
"NumberofColumns" : 39,
"ColName": {
"months_as_customer" : "Integer",
"age": "Integer",
"policy_number": "Integer",
"policy_bind_date": "varchar",
"policy_state": "varchar",
"policy_csl": "varchar",
"policy_deductable": "Integer",
"policy_annual_premium": "Integer",
"umbrella_limit": "Integer",
"insured_zip": "Integer",
"insured_sex": "varchar",
"insured_education_level": "varchar",
"insured_occupation": "varchar",
"insured_hobbies": "varchar",
"insured_relationship": "varchar",
"capital_gains": "Integer",
"capital_loss": "Integer",
"incident_date": "varchar",
"incident_type": "varchar",
"collision_type": "varchar",
"incident_severity": "varchar",
"authorities_contacted": "varchar",
"incident_state": "varchar",
"incident_city": "varchar",
"incident_location": "varchar",
"incident_hour_of_the_day": "Integer",
"number_of_vehicles_involved": "Integer",
"property_damage": "varchar",
"bodily_injuries": "Integer",
"witnesses": "Integer",
"police_report_available": "varchar",
"total_claim_amount": "Integer",
"injury_claim": "Integer",
"property_claim": "Integer",
"vehicle_claim": "Integer",
"auto_make": "varchar",
"auto_model": "varchar",
"auto_year": "Integer",

}
}



ob = App_Logger()

class Prediction_Data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.
               """

    def __init__(self):
#        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'



    def valuesFromSchema(self):
        """
                                Method Name: valuesFromSchema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception
                                        """
        try:
            with open(self.schema_path, 'w') as f:
                f.write(json.dumps(dicts1))
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open('Prediction_Logs/valuesfromSchemaValidationLog.txt', 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            ob.log(file,message)

            file.close()



        except ValueError:
            file = open('Prediction_Logs/valuesfromSchemaValidationLog.txt', 'a+')
            ob.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open('Prediction_Logs/valuesfromSchemaValidationLog.txt', 'a+')
            ob.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open('Prediction_Logs/valuesfromSchemaValidationLog.txt', 'a+')
            ob.log(file, str(e))
            file.close()
#            raise e
            raise AppException(e, sys) from e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        """
                                      Method Name: manualRegexCreation
                                      Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                  This Regex is used to validate the filename of the prediction data.
                                      Output: Regex pattern
                                      On Failure: None
                                              """
        regex = "['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                        Method Name: createDirectoryForGoodBadRawData
                                        Description: This method creates directories to store the Good Data and Bad Data
                                                      after validating the prediction data.

                                        Output: None
                                        On Failure: OSError
                                                """
        try:
#            directory = 'Good_Raw/'
#            parent_dir = 'Prediction_Raw_Files_Validated/'
#            path = os.path.join(parent_dir,directory)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
#            directory1 = 'Bad_Raw/'
#            parent_dir1 = 'Prediction_Raw_Files_Validated/'
#            path1 = os.path.join(parent_dir1,directory1)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            ob.log(file,"Error while creating Directory %s:" % ex)

            file.close()
            raise OSError
        except Exception as e:
            raise AppException(e, sys) from e


    def deleteExistingGoodDataTrainingFolder(self):
        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.


                                                    """
        try:
            location = 'Prediction_Raw_Files_Validated/'
            dirs = 'Good_Raw/'
            path = os.path.join(location,dirs)
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                ob.log(file,"GoodRaw directory deleted successfully!!!")

                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            ob.log(file,"Error while Deleting Directory : %s" %s)

            file.close()
            raise OSError
        except Exception as e:
            raise AppException(e, sys) from e
    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError
                                                    """

        try:
            location1 = 'Prediction_Raw_Files_Validated/'
            dirs1 = 'Bad_Raw/'
            path = os.path.join(location1,dirs1)
            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                ob.log(file,"BadRaw directory deleted before starting validation!!!")

                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            ob.log(file,"Error while Deleting Directory : %s" %s)

            file.close()
            raise OSError
        except Exception as e:
            raise AppException(e, sys) from e

    def moveBadFilesToArchiveBad(self):


        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError
                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            path = "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            dest = 'PredictionArchivedBadData/BadData_' + str(date) + "_" + str(time)
#            directory = 'BadData''_' + str(date) + "_" + str(time)
#            dirs = 'Bad_Raw/'
#            parent_dir = ('pythonmyproject/PredictionArchivedBadData')
            if not os.path.isdir(dest):
                os.makedirs(dest)
#            dest = os.path.join(parent_dir, directory)
#            if os.path.isdir(source):
#               print('Good')
#               if not os.path.isdir(parent_dir):
#                   os.makedirs(parent_dir)
#              if not os.path.isdir(dest):
#                 os.makedirs(dest)
            filesonly = os.listdir(source)
            for f in filesonly:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest+f)
                    print('gd to proceed')
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            ob.log(file,"Bad files moved to archive")
            file.close()
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            ob.log(file,"Bad Raw Data Folder Deleted successfully!!")

            file.close()
        except Exception as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            ob.log(file,"Error while moving bad files to archive:: %s" % e)

            file.close()
#            raise e
#        except Exception as e:
            raise AppException(e, sys) from e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the prediction csv file as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception
        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        try:
            path = 'Prediction_Batch_files/'
            f = open('Prediction_Logs/nameValidationLog.txt', 'a+')
            for i in os.listdir(path):
                if (re.match(regex, i)):
                    splitAtDot = (re.split('.csv', i)) #if file is csv then will create regex with .csv extension else .txt will use
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
#                            shutil.copy((r'pythonmyproject\Prediction_Batch_files\\' + i ), (r"pythonmyproject\Prediction_Raw_Files_Validated\Good_Raw"))
                            shutil.copy("Prediction_Batch_files/" + i, "Prediction_Raw_Files_Validated/Good_Raw")
                            ob.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % i)
                        else:
#                            shutil.copy((r'pythonmyproject\Prediction_Batch_Files\\' + i ), (r"pythonmyproject\Prediction_Raw_Files_Validated\Bad_Raw"))
                            shutil.copy("Prediction_Batch_files/" + i, "Prediction_Raw_Files_Validated/Bad_Raw")
                            ob.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % i)

                    else:
                        shutil.copy("Prediction_Batch_files/" + i, "Prediction_Raw_Files_Validated/Bad_Raw")
#                        shutil.copy((r'pythonmyproject\Prediction_Batch_Files\\' + i ), (r"pythonmyproject\Prediction_Raw_Files_Validated\Bad_Raw"))
                        ob.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % i)

                else:
                    shutil.copy("Prediction_Batch_files/" + i, "Prediction_Raw_Files_Validated/Bad_Raw")
#                    shutil.copy((r'pythonmyproject\Prediction_Batch_Files\\' + i ), (r"pythonmyproject\Prediction_Raw_Files_Validated\Bad_Raw"))
                    ob.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % i)


            f.close()

        except Exception as e:
            f = open('Prediction_Logs/nameValidationLog.txt', 'a+')
            ob.log(f,"Error occured while validating FileName %s" % e)

            f.close()
#            raise e
#        except Exception as e:
            raise AppException(e, sys) from e



    def validateColumnLength(self,NumberofColumns):
        """
                    Method Name: validateColumnLength
                    Description: This function validates the number of columns in the csv files.
                                 It is should be same as given in the schema file.
                                 If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                 If the column number matches, file is kept in Good Raw Data for processing.
                                The csv file is missing the first column name, this function changes the missing name to "Wafer".
                    Output: None
                    On Failure: Exception
             """
        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            ob.log(f,"Column Length Validation Started!!")

            for file in os.listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv('Prediction_Raw_Files_Validated/Good_Raw/' + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file, "Prediction_Raw_Files_Validated/Bad_Raw")
                    ob.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)


            ob.log(f,"Column Length Validation Completed!!")

        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            ob.log(f,"Error Occured while moving the file :: %s" % OSError)

            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            ob.log(f,"Error Occured:: %s" % e)

            f.close()
#            raise e
 #       except Exception as e:
            raise AppException(e, sys) from e

        f.close()

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_file/Predictions.csv'):
            os.remove('Prediction_Output_file/Predictions.csv')

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception
                              """
        try:
            f = open('Prediction_Logs/ValuesInColumn.txt','a+')
            ob.log(f,"Missing Values Validation Started!!")


            for file in listdir("Prediction_Raw_Files_Validated/Good_Raw/"):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                        ob.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

                        break
                if count==0:
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open('Prediction_Logs/missingValuesInColumn.txt','a+')
            ob.log(f, "Error Occured while moving the file :: %s" % OSError)

            f.close()
            raise OSError
        except Exception as e:
            f = open('Prediction_Logs/missingValuesInColumn.txt','a+')
            ob.log(f, "Error Occured:: %s" % e)

            f.close()
#            raise e
#        except Exception as e:
            raise AppException(e, sys) from e
        f.close()













