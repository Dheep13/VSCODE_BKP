from turtle import position
import xdrlib
from flask import send_from_directory , abort, Flask, send_file, request, jsonify, session
import os
import json
import requests
import credentials as cred

app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/GetUniqueInvoiceId')
def hello():
    
    if (request.method == 'POST'):
    ##########      GET SLUG        ##############
        botdata = json.loads(request.get_data())
        with open('botdata.json', 'w') as outfile:
            json.dump(botdata, outfile)
            
    positionseq=botdata['conversation']['memory']['positionseq']
    periodseq=botdata['conversation']['memory']['periodseq']
    base_url = "https://0509.callidusondemand.com/api/v2/Measurements?$filter=(position eq "+positionseq +" and period eq " +periodseq+ " )"
    r = requests.get(base_url , auth=(cred.username,cred.password))
    pos_data = json.loads(r.text)

    with open('pos_data.json', 'w') as outfile:
        json.dump(pos_data, outfile)

    salesorderseq=[]
    for x in pos_data['credits']:
        if(x["salesOrder"] not in salesorderseq):
            salesorderseq.append(x["salesOrder"])
    
    print(salesorderseq)
    
    for i in salesorderseq:
        base_url = "https://0509.callidusondemand.com/api/v2/salesOrders?$filter=(salesOrderSeq eq "+i+ ")"
        r = requests.get(base_url , auth=(cred.username,cred.password))
        order = json.loads(r.text)

        with open('order.json', 'w') as outfile:
            json.dump(order, outfile)

        orderid=[]
        for x in order['salesOrders']:
            orderid.append(x["orderId"])
    
    return jsonify(
        status=200,
        replies=[{
        'type': 'text',
        'content': 'The SalesOrderId is %s' % (orderid[0])
        }]
        ) 

if __name__ == '__main__':
    app.run(host="localhost", port=4000, debug=True)
	# if cf_port is None:
	# 	app.run(host='0.0.0.0', port=5000, debug=True)
	# else:
	# 	app.run(host='0.0.0.0', port=int(cf_port), debug=True)