import requests
import json
import pandas as pd

# Load APP_TOKEN vairable from keys file
from secret import APP_TOKEN

# object for data to be loaded to database
# params should be a list of strings of the API field names to be queried
# startyear should be a string for the year we want to start the query 
# endyear should be the string for the year we want to end the query  
class CrimeData:

    def __init__(self, params, startyear, endyear):
        # turn params into comma seperated string attribute
        self.params = ''
        for i in range(0,len(params)):
            if i == len(params)-1:
                self.params += params[i]
                break
            else:
                self.params += params[i] + ','
            
        # Starting year of data we want to query
        self.startyear = startyear
        # Ending year of data we want to query
        self.endyear = endyear

    def get_data(self,limit='10000'):
        # String literal 
        url = f'https://data.cityofchicago.org/resource/ijzp-q8t2.json?$select={self.params}&$where=year<={self.endyear} AND year >= {self.startyear}&$order=date ASC&$limit={limit}'
        # Using our app token
        headers = {'Accept': 'application/json', 'X-App-Token': APP_TOKEN}
        resp = requests.get(url,headers=headers)
        df = json.loads(resp.text)

        # Return the queried data excluding na rows
        self.data = pd.DataFrame(df).dropna()
        return self.data
    
def main():
    test = CrimeData(params=['date','community_area'],startyear='2004',endyear='2005')
    test.get_data(limit='10000')
    print(test.params)
    print(test.startyear)
    print(test.endyear)
    print(test.data)

if __name__ == "__main__":
    main()