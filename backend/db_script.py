from datetime import datetime
import requests
import json
import pandas as pd
import pymysql
from sqlalchemy import create_engine

# Load APP_TOKEN vairable from keys file
from secret import APP_TOKEN

# Load variable from databse credentials file
from db_creds import HOST,PORT,USER,PASSW,DATABASE

# object for data to be loaded to database
# params should be a list of strings of the API field names to be queried
# startyear should be a string for the year we want to start the query 
# endyear should be the string for the year we want to end the query  
class CrimeData:

    def __init__(self, params):
        # turn params into comma seperated string attribute
        self.params = ''
        for i in range(0,len(params)):
            if i == len(params)-1:
                self.params += params[i]
                break
            else:
                self.params += params[i] + ','

        # create engine for database connection
        self.mydb = create_engine('mysql+pymysql://' + USER + ':' + PASSW + '@' + HOST + ':' + str(PORT) + '/' + DATABASE, echo=False)

    # Retreive data from Socrata API
    def get_API_year_data(self, startyear, endyear, limit='10000'):
        # String literal 
        url = f'https://data.cityofchicago.org/resource/ijzp-q8t2.json?$select={self.params}&$where=year<={endyear} AND year >= {startyear}&$order=date ASC&$limit={limit}'
        # Using our app token
        headers = {'Accept': 'application/json', 'X-App-Token': APP_TOKEN}
        resp = requests.get(url,headers=headers)
        df = json.loads(resp.text)

        # Return the queried data
        self.year_data = pd.DataFrame(df).dropna()

        # typecasting
        self.year_data['date'] = pd.to_datetime(self.year_data['date'])
        self.year_data['community_area'] = self.year_data['community_area'].astype(int)

        return self.year_data
    
    # Send data to a table in database (replace an existing table if it exists or creates a new one)
    def send_year_data(self,tablename):
        self.year_data.to_sql(name=tablename, con=self.mydb, if_exists='replace', index=False)

def main():
    new = CrimeData(params=['date','community_area'])
    new.get_API_year_data(startyear='2010',endyear='2011',limit='10000000')
    new.send_year_data(tablename='test')

if __name__ == "__main__":
    main()