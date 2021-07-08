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
        self.params = ''
        for i in range(0,len(params)):
            if i == len(params)-1:
                paramsstring = params[i]
                self.params += paramsstring
                break
            else:
                paramsstring = params[i] + ','
                self.params += paramsstring
            
        self.startyear = startyear
        self.endyear = endyear
    
def main():
    test = CrimeData(['hi','test'],'2010','2018')
    print(test.params)
    print(test.startyear)
    print(test.endyear)

if __name__ == "__main__":
    main()