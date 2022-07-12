from datetime import datetime
from os import listdir #needs to write the code in my way
import pandas
from application_logging.logger import App_Logger
import sys
import os
from app_exception.exception import AppException

ob = App_Logger()
import os
class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
                  """
     def __init__(self):
          self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw" # give the path



     def replaceMissingWithNull(self):

          """
                                  Method Name: replaceMissingWithNull
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.
                                          """
          try:
               log_file = open('Prediction_Logs/dataTransformLog.txt','a+')
               source = self.goodDataPath
               for file in os.listdir(source):
                    data = pandas.read_csv(self.goodDataPath + "/" + file)
                    # list of columns with string datatype variables
                    columns = ["policy_bind_date","policy_state","policy_csl","insured_sex","insured_education_level","insured_occupation","insured_hobbies","insured_relationship","incident_state","incident_date","incident_type","collision_type","incident_severity","authorities_contacted","incident_city","incident_location","property_damage","police_report_available","auto_make","auto_model"]

                    for col in columns:
                         data[col] = data[col].apply(lambda x: "'" + str(x) + "'") #putting single quotes to all the string data present in all the columns
                    data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
               ob.log(log_file,'File Transformed successfully!!')
               log_file.close()

          except Exception as e:
               log_file = open('Prediction_Logs/dataTransformLog.txt','a+')
               ob.log(log_file,"Data Transformation failed because:: %s" % e)
               log_file.close()

#               raise e
#          except Exception as e:
               raise AppException(e, sys) from e

