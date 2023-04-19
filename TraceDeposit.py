
from operator import eq
from flask import Flask, request, jsonify
import os
import json
import requests 

app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/PolicyInfo',methods=['POST','GET'])
def bot():
    creditSeqList=[]
    url = "https://0509.callidusondemand.com/"
    Username='Deepan'
    Password='Msd183$$'
    SalesOrderAPI='/api/v2/salesOrders'
    CreditAPI='/api/v2/credits'
    base_url = url+SalesOrderAPI+"?$filter=orderId eq SM921P&select=salesOrderSeq"
    r = requests.get(base_url , auth=(Username,Password))
    print(r.text)
    OrderSeq_data = json.loads(r.text)
    for x in OrderSeq_data['salesOrders']:
            #positionName=x['name']
            #print(x['name']) 
            y = (x['salesOrderSeq'])
            # return y
    base_url = url+CreditAPI+"?$filter=salesOrder eq "+y+"&select=creditSeq"
    r = requests.get(base_url , auth=(Username,Password))
    print(r.text)
    creditSeq_data = json.loads(r.text)
    for x in creditSeq_data['credits']:
        creditSeqList.append(x['creditSeq'])
        print (creditSeqList)
        #return jsonify(creditSeqList)

    for x in creditSeqList:
        print (x)



if __name__ == '__main__':
	if cf_port is None:
		app.run(host='localhost', port=5000, debug=True)
	else:
		app.run(host='localhost', port=int(cf_port), debug=True)
  
  
  https://0509.callidusondemand.com/api/v2/measurements?$filter=measurementSeq eq [18014398509499077]