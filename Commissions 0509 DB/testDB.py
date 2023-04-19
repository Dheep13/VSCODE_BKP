#Following is the example of connecting to database  
#Import module
from hdbcli import dbapi
import 
#Open the database conenciton 
conn = dbapi.connect(address="10.119.28.81", port=30204,user="ps_iad0509sbox", password="PKJH58cvbn")
# prepare a cursor object using cursor() method
cursor = conn.cursor()
#print("Hello")
sql= 'select eventtypeid, datatypeseq from cs_eventtype'
#sql = 'SELECT * FROM T1'
#cursor = conn.cursor()
#print("Hello")
cursor.execute(sql)
#print("Hello")
for row in cursor:
    print (row.replace('(', ''))
# disconnect from server
conn.close()
#------------------END--------------