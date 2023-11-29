import requests
import pandas
import sqlalchemy
import pyodbc
import os
from dotenv import load_dotenv, find_dotenv



#the variables below should be added to config file so that we don't have to touch this code once developed 
dotenv_path=find_dotenv()
load_dotenv(dotenv_path)
url_base = os.getenv("url_base_productdecoderring")
api_key = os.getenv("api_key_productdecoderring")

#this can be json or csv
format_identifier = "json"
url = f"{url_base}?api_key={api_key}&format={format_identifier}".format(url_base, api_key, format_identifier)
payload = {}
headers = {}
response = requests.request("GET", url, headers=headers, data=payload)


#here you will need to write the logic to add it to the schema in the database that you guys have created the model for
#print(response.text)

#print respose in json content
responseData = response.json()
#print(responseData)

#normalize json dataset called 'product_decoder_ring'
df = pandas.json_normalize(responseData,'product_decoder_ring')
print(df)

#create connection between python and SQL Server, server name "g02d40usgdb01.dev.us.corp", database name"ValueStream"
server = os.getenv("ServiceNow_ServerName")
db = os.getenv("ServiceNow_DB_Name")
uid = os.getenv("ServiceNow_DB_UserName")
pwd = os.getenv("ServiceNow_DB_Pwd")
driver = 'ODBC Driver 17 for SQL Server'
database_connection = f'mssql://{uid}:{pwd}@{server}/{db}?driver={driver}'

engine=sqlalchemy.create_engine(database_connection)

#push data stored in a dataframe to SQL Server table "valuestream"
df.to_sql(name='ProductDecoderRing',con=engine,index=False,if_exists='fail')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(response.text)
