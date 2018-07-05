import pandas as pd
import urllib
from sqlalchemy import create_engine
import time


serverName = 'localhost'
userName = 'postgres'
port = '5432'
passWord = 'postgres'
driver = ''
dbName = 'dellstore'

#quoted = urllib.quote_plus('DRIVER={' + driver + '};SERVER=' + serverName + ';DATABASE='+dbName+';UID=' + userName + ';PWD=' + passWord + ';PORT='+ port + '')
#db_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))


db_engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (userName, passWord, serverName, port, dbName))

#query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
query = "Select * from customers"

print pd.read_sql(query ,db_engine)
print time.clock()

#rowCount = pd.read_sql("SELECT COUNT(*) FROM customers ;", con=db_engine)
#count = rowCount.iloc[0]['count']
#print str(count)
#print("Total Rows:"+ rowCount)

print time.clock()
lines_number = 20000
lines_in_chunk = 5000
counter = 0
completed = 0
index = 0
frames = pd.DataFrame()

for df in pd.read_sql(query ,db_engine,chunksize=lines_in_chunk):
    counter += lines_in_chunk
    new_completed = int(round(float(counter) / lines_number * 100))
    print len(df),type(df)

    print len(frames) ,len(df)
    if len(df) >= 0:
        frames = frames.append(df,ignore_index=True)
        print True
    else:
        print False
    if new_completed > completed :
        completed = new_completed
        print "Completed", completed, "%"
    index += 1
#
name = get_df_name(frames)
print name ,time.clock()

def get_df_name():

    name =[x for x in globals() if globals()[x] is data][0]

    return name



