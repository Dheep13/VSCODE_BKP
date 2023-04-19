#Following is the example of connecting to database  
#Import module
from hdbcli import dbapi
import json
import os
import credentials as cr
from flask import send_from_directory , abort, Flask, send_file, request, jsonify, session

app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/order/<orderid>')
def cs_order(orderid):
    #Open the database conenciton 
    conn = dbapi.connect(address="10.119.28.81", port=30204,user=cr.username, password=cr.password)
    # prepare a cursor object using cursor() method
    cursor = conn.cursor()
    # disconnect from server
    creditseq_lst=[]
    sql = "SELECT DISTINCT MES.NAME, MES.VALUE, PER.NAME, SO.SALESORDERSEQ FROM CS_SALESORDER SO LEFT OUTER JOIN CS_SALESTRANSACTION ST ON SO.SALESORDERSEQ = ST.SALESORDERSEQ LEFT OUTER JOIN CS_CREDIT CR ON CR.SALESORDERSEQ=ST.SALESORDERSEQ AND CR.SALESORDERSEQ = SO.SALESORDERSEQ AND ST.SALESTRANSACTIONSEQ=CR.SALESTRANSACTIONSEQ LEFT OUTER JOIN CS_PMCREDITTRACE PMC ON PMC.CREDITSEQ = CR.CREDITSEQ AND PMC.CONTRIBUTIONVALUE <>0.0 LEFT OUTER JOIN CS_MEASUREMENT MES ON PMC.MEASUREMENTSEQ = MES.MEASUREMENTSEQ AND MES.VALUE <>0.0 LEFT OUTER JOIN CS_PERIOD PER ON MES.PERIODSEQ=PER.PERIODSEQ AND PER.REMOVEDATE > CURRENT_DATE where so.orderid = " + "'" + str(orderid) + "'" + " and pmc.contributionvalue<>0.0"
    print(sql)
    cursor.execute(sql)
    i=1
    for row in cursor:
        creditseq_lst.insert(i,row)
    print(creditseq_lst)
    conn.close()
    return(json.dumps([{"MEASUREMENTNAME": ip[0],"VALUE": str(ip[1]),"PERIOD": str(ip[2]) } for ip in creditseq_lst]))

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)
#------------------END--------------