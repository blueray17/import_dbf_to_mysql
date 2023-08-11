import pandas as pd
from simpledbf import Dbf5
import sqlalchemy
import glob


database_username = 'root'
database_password = ''
database_ip       = 'localhost'
database_name     = 'gmapsscrapp'

def import_ke_mysql(df, filename):
    
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name))
    df.to_sql(con=database_connection, name=filename, if_exists='replace')

dbffiles = []
for file in glob.glob("*.dbf"):
    dbffiles.append(file)
print("jumlah file : ", len(dbffiles))

sep = '.'

dbffiles = []
for file in glob.glob("*.dbf"):
    stripped = file.split(sep, 1)[0]
    dbffiles.append(stripped)
print("jumlah file : ", len(dbffiles))

for x in range(len(dbffiles)):
    print(x,". ",dbffiles[x])
    namafile = dbffiles[x]+".dbf"
    dbf = Dbf5(namafile)
    df = dbf.to_dataframe()
    dfd = df.head(n=10)
    #print(df)
    import_ke_mysql(df,dbffiles[x])
    

