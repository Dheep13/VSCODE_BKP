import xdrlib
from flask import send_from_directory , abort, Flask, send_file, request, jsonify, session
from pathlib import Path
import pathlib
import os
import json
import requests
import credentials as cred
import urllib.request
import io


base_url = "https://0509.callidusondemand.com/api/v2/credits?$filter=(position eq 4785074604081855 and period eq 2533274790396152 )"
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
print (orderid)
