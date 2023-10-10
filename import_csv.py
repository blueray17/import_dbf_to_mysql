import pandas as pd
import sqlalchemy
import glob
import psycopg2


def import_ke_postgresql(df, filename):
    #conn = psycopg2.connect(database="podesbps",host="localhost",user="postgres",password="ipin",port="5432")
    database_connection = sqlalchemy.create_engine('postgresql+psycopg2://postgres:ipin@localhost:5432/podesbps')

    df.to_sql(filename, database_connection, if_exists='replace', index=False)

csvfiles = []
for file in glob.glob("*.csv"):
    csvfiles.append(file)
print("jumlah file : ", len(csvfiles))

sep = '.'

csvfiles = []
for file in glob.glob("*.csv"):
    stripped = file.split(sep, 1)[0]
    csvfiles.append(stripped)
print("jumlah file : ", len(csvfiles))

for x in range(len(csvfiles)):
    print(x,". ",csvfiles[x])
    namafile = csvfiles[x]+".csv"
    df = pd.read_csv(namafile, sep=",", header=0)
    dfd = df.head(n=1)
    print(dfd)
#    import_ke_mysql(df,csvfiles[x])
    import_ke_postgresql(df,csvfiles[x]) 