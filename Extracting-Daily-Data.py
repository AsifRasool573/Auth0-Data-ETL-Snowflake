import pandas as pd


import http.client
import json
from pandas.io.json import json_normalize


import datetime as dt
from datetime import date

import snowflake.connector
import sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL



# ********************Client IDs and Secrets******************************* #




client_id = 'YOUR CLIENT ID'
client_secret = 'YOUR CLIENT SECRET'
business_name = 'BUSINESS NAME AND ENVIRONMENT'



# ********************getting the access token******************************* #


conn = http.client.HTTPSConnection(business_name+".REGION AND AUTH0 DETAILS")

payload = "{\"client_id\":\""+client_id+"\",\"client_secret\":\""+client_secret+"\",\"audience\":\"https://"+business_name+"REGION AND AUTH0 DETAILS/api/v2/\",\"grant_type\":\"client_credentials\"}"

headers = { 'content-type': "application/json" }

conn.request("POST", "/oauth/token", payload, headers)

res = conn.getresponse()
data = res.read()
# data

tokendetails_json = data.decode('utf8').replace("'", '"')
# tokendetails_json

tokendetails = json.loads(tokendetails_json)
print(tokendetails)



# ********************Connection to the Data Warehouse******************************* #


# engine = create_engine(URL(
#         account='ACCOUNT NAME',
#         user='USERNAME',
#         password='PASSWORD',
#         database='DATABASE',
#         schema='SCHEMA',
#         warehouse = 'DATA WAREHOUSE'
#     ))
# connection = engine.connect()


# print('\n Data Warehouse Connection Done.....')



# ********************Setting Header for API Call******************************* #




headers = { 'content-type': "application/json","Authorization": "Bearer "+tokendetails['access_token'] }




# ********************Data Cleaning and Pushing to Snowfalke******************************* #

dirty_data = []

column_list = [REQUIRED COLUMN NAMES, IF YOU DO NOT KNOW THEN CHECK FROM JSON DATA WHICH IS BEING RETURNED BY THE API CALL.]

column_list = [x.upper() for x in column_list]



def data_cleaning(dirty_data):
    
    final_dataframe = pd.DataFrame()
    
    
    for item in dirty_data:
      MAP THE DATA TO VARIABLES AND PUT THESE VARIABLES IN TO A LIST AND IF YOU DO NOT KNOW THE HIRARECY OF THE DATA THEN CHECK IN THE JSON. TRY TO PUT SOME VARIABLES
      IN TRY EXCEPT IF YOU DOUBT. ALSO ASSIGN SOME DEFAULT VALUES TO AVOID ANY ISSUE.
        
        
            
        
        record_list = [ALL VARAIBLES WHICH YOU MAPPED IN ABOVE LOOP]
        #dataframe_creation(record_list)


        data_df = pd.DataFrame(record_list)
        data_df = data_df.T
        data_df.columns = column_list
        #print(data_df)
        final_dataframe = pd.concat([final_dataframe, data_df])

    

    final_dataframe.to_sql('TABLE NAME', con=engine, index=False, if_exists='append')

    

        
# ********************Extracting the Users Data and sending to Cleaning Function******************************* #



updated_date = str(date.today() - dt.timedelta(days=1))

pagenumber = 0
flag = True

while(flag==True):

    conn.request("GET", "/api/v2/users?q=updated_at%3A%22"+updated_date+"%22&include_totals=true&page="+str(pagenumber)+"&search_engine=v3", payload, headers)
    res = conn.getresponse()
    data = res.read()

    result_data_json = data.decode('utf8')
    # .replace("'", '"')

    data = json.loads(result_data_json)

    user_data = data['users']
    print(user_data)
    limit = data['length']


    print(f"Page Number:{pagenumber}, Limit: {limit}")
    if (limit == 50):
        pagenumber += 1
        data_cleaning(user_data)

#         print(f'{limit} Records are added to the Dataframe \n\n\n')
    else:
        flag=False
        data_cleaning(user_data)

        
# ********************Data is Pushed to Snowflake******************************* #
        
        
print(f"{updated_date} done")
