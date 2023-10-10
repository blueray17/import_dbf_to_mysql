import glob

sep = '.'

dbffiles = []
for file in glob.glob("*.dbf"):
    stripped = file.split(sep, 1)[0]
    dbffiles.append(stripped)
print("jumlah file : ", len(dbffiles))

for x in range(len(dbffiles)):
    print(dbffiles[x])