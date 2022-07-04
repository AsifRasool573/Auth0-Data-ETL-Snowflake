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




client_id = 'YOUR AUTH0 CLIENT ID'
client_secret = 'CLIENT SECRET'
business_name = 'BUSINESS ENVIRONMENT NAME'



# ********************getting the access token******************************* #


conn = http.client.HTTPSConnection(business_name+".BUSINESS DOMAIN WITH REGION AND AUTH0.")

payload = "{\"client_id\":\""+client_id+"\",\"client_secret\":\""+client_secret+"\",\"audience\":\"https://"+business_name+".eu.auth0.com/api/v2/\",\"grant_type\":\"client_credentials\"}"

headers = { 'content-type': "application/json" }

conn.request("POST", "/oauth/token", payload, headers)

res = conn.getresponse()
data = res.read()
# data

tokendetails_json = data.decode('utf8').replace("'", '"')
# tokendetails_json

tokendetails = json.loads(tokendetails_json)
print(tokendetails)




engine = create_engine(URL(
        account='SNOWFLAKE ACCOUNT NAME',
        user='USERNAME',
        password='PASSWORD',
        database='DATABASE',
        schema='SCHEMA',
        warehouse = 'WAREHOUSE'
    ))
connection = engine.connect()


print('\n Data Warehouse Connection Done.....')


# final_data.to_sql('TABLE NAME', con=engine, index=False, if_exists='append')




# ********************Extracting the Users Data******************************* #


# conn = http.client.HTTPSConnection(business_name+".eu.auth0.com")
# payload = "{\"client_id\":\""+client_id+"\",\"client_secret\":\""+client_secret+"\",\"audience\":\"https://"+business_name+".eu.auth0.com/api/v2/\",\"grant_type\":\"client_credentials\"}"

dirty_data = []





column_list = [ENTER YOUR REQUIRED COLUMN NAME FROM THE DATA. IF YOU DO NOT KNOW THEN PRINT THE JSON AND CHECK WHAT KIND OF DATA IS BEING RETURNED]

column_list = [x.upper() for x in column_list]


headers = { 'content-type': "application/json","Authorization": "Bearer "+tokendetails['access_token'] }





# creation_date = str(date.today() - dt.timedelta(days=3))
# creation_data = '2021-11-03'



def data_cleaning(dirty_data):
    
    final_dataframe = pd.DataFrame()
    
    
    for item in dirty_data:
          CHECK YOUR JSON DATA AND MAP THE DATA FROM JSON TO ALL PARTICULAR VARIABLES WHICH YOU HAVE TO SEND TO THE DATABASE/DATA WAREHOUSE.
          PUT SOME COLUMNS IN TRY CATCH SITUATION IF YOU THINK THEY CAN CREATE PROBLEN, IF SO, THEN ASSIGN THEM DEFAULT VALUE OR BLANK.

            
        
        record_list = [HERE COMES ALL THE LIST OF ALL TEH COLUMNS IN THE DATA TO BE SEND TO THE DATA WARE HOUSE Or DATABASE.]
       
      
        #dataframe_creatioN


        data_df = pd.DataFrame(record_list)
        data_df = data_df.T
        data_df.columns = column_list
        #print(data_df)
        final_dataframe = pd.concat([final_dataframe, data_df])

    

    final_dataframe.to_sql('AUTH0_USERS', con=engine, index=False, if_exists='append')

    

        
#### End of data_cleaning function




datelist = pd.date_range(start='ENTER START DATE', end=date.today() - dt.timedelta(days=2)).to_pydatetime().tolist()
datelist = [str(datelist[x].date()) for x in range(len(datelist))]

for updated_date in datelist:
    
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
        limit = data['length']


    #     print(f"Page Number:{pagenumber}, Limit: {limit}")
        if (limit == 50):
            pagenumber += 1
            data_cleaning(user_data)

    #         print(f'{limit} Records are added to the Dataframe \n\n\n')
        else:
            flag=False
            data_cleaning(user_data)
            
    print(f"{updated_date} done")
  
    
