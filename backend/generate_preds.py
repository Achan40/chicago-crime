import pandas as pd
import pymysql
from sqlalchemy import create_engine

# Load variable from databse credentials file
from db_creds import HOST,PORT,USER,PASSW,DATABASE

class CrimePreds:

    # Create engine and connection objects
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://' + USER + ':' + PASSW + '@' + HOST + ':' + str(PORT) + '/' + DATABASE, echo=False)
        self.conn = self.engine.connect()
        
    # Retreive data from our database
    def get_data(self, tablename):
        query_string = f"SELECT date, COUNT(*) AS num_crimes FROM {tablename} GROUP BY YEAR(date), MONTH(date)"
        self.data = pd.read_sql(query_string, self.conn)

def main():
    test = CrimePreds()
    test.get_data('test')
    print(test.data)
    print(test.data.dtypes)
    print(test.data['num_crimes'].sum())

if __name__ == "__main__":
    main()