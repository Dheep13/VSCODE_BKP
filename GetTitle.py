from flask import send_from_directory , abort, Flask, send_file, request, jsonify, session
from pathlib import Path
import pathlib
import os
import json
import requests
import CREDENTIALS
from PIL import Image
import urllib.request
import io
import pandas as pd
import matplotlib.pyplot as plt
import datetime


app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/')
def hello():
    return '<h1>SAP Conversational AI</h1><body>The Commissions GetTitle webhook for use in SAP Conversational AI chatbots.<br><img src="https://picsum.photos/200/300"></body>'

@app.route('/Title',methods=['POST','GET'])
def bot(page):
   
    if (request.method == 'POST'):
        ##########      GET SLUG        ##############
        data = json.loads(request.get_data())
        with open('data.json', 'w') as outfile:
            json.dump(data, outfile)
            skill = data['conversation']['skill']
            with open('skill.json', 'w') as outfile:
                json.dump(skill, outfile)
        ##########      GET TITLE        ##############
        if final_slug == 'gettitle':
            position_value = data['conversation']['memory']['position']['raw']
            with open('pos_value.json', 'w') as outfile:
                json.dump(position_value, outfile)
            base_url = "https://0509.callidusondemand.com/api/v2/positions?$filter=name eq "+position_value+" &select=name,title&expand=title"
            r = requests.get(base_url , auth=(CREDENTIALS.api_login,CREDENTIALS.api_password))
            pos_data = json.loads(r.text)

            with open('pos_data.json', 'w') as outfile:
                json.dump(pos_data, outfile)

            for x in pos_data['positions']:
                positionName=x['name']
                print(x['name']) 
                y = (x['title'])
                TitleName=y['displayName']
                print(y['displayName'])

            return jsonify(
            status=200,
            replies=[{
            'type': 'text',
            'content': 'The title name is %s' % (TitleName)
            #   'content': 'The title name is %s ' %(base_url)
            }]
            ) 

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)
