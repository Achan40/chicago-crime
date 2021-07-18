from datetime import datetime
from os import pread
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
        self.engine = create_engine('mysql+pymysql://' + USER + ':' + PASSW + '@' + HOST + ':' + str(PORT) + '/' + DATABASE, echo=False)
        self.conn = self.engine.connect()

    # Retreive data from Socrata API
    def get_yearly_data(self, startyear, endyear, limit='10000'):
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
    def send_yearly_data(self, tablename):
        self.year_data.to_sql(name=tablename, con=self.engine, if_exists='replace', index=False)

    # Retrieve the most recent entry date of a table in our database
    def get_most_recent_date(self, tablename):
        query_string = f"SELECT MAX(date) AS date FROM {tablename}"
        self.most_recent_date = pd.read_sql(query_string, self.conn)
        self.most_recent_date['date'] = self.most_recent_date['date'].astype(str)
        self.most_recent_date = self.most_recent_date['date'][0]
        # Correct formatting to so that we can use in API request
        self.most_recent_date = self.most_recent_date.replace(' ','T')
        return self.most_recent_date

    # Given a table in our database, retreive new data from Socrata API
    def get_updates(self, tablename, limit='10000'):
        most_r = self.get_most_recent_date(tablename=tablename)

        # String literal
        url = f'https://data.cityofchicago.org/resource/ijzp-q8t2.json?$select={self.params}&$where=date > \'{most_r}\' &$order=date ASC&$limit={limit}'
        # Using our app token
        headers = {'Accept': 'application/json', 'X-App-Token': APP_TOKEN}
        resp = requests.get(url,headers=headers)
        df = json.loads(resp.text)

        # Return queried data
        self.new_data = pd.DataFrame(df).dropna()

        # typecasting
        self.new_data['date'] = pd.to_datetime(self.new_data['date'])
        self.new_data['community_area'] = self.new_data['community_area'].astype(int)

        return self.new_data

    # Send the new data 
    def send_updates(self, tablename):
        self.new_data.to_sql(name=tablename, con=self.engine, if_exists='append', index=False)


def main():
    new = CrimeData(params=['date','community_area'])
    # new.get_yearly_data(startyear='2010',endyear='2011',limit='10000000')
    # new.send_yearly_data(tablename='test')
    new.get_updates('test')
    new.send_updates('test')

if __name__ == "__main__":
    main()