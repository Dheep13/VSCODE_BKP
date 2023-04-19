# integrating CAI with Commissions using slack
# Use case : get measurement from two positions and compare using pie chart

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

app = Flask(__name__)
port = '5000'
app.config["CLIENT_IMAGES"] = pathlib.Path().absolute()
app.config["SECRET_KEY"] = "23432545@@@$$&&###^#$#aafdfssfs"
# app.config["SESSION_PERMANENT"] = True

s=os.path.join(pathlib.Path().absolute(),'iceCream.jpg')
p=os.path.join(pathlib.Path().absolute(),'DepositCompare.png')
print(s)


@app.after_request
def after_request(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response

@app.route('/<page>',methods=['POST','GET'])
def get_info(page):
   
    # data = json.loads(request.())
    # if request.method == 'GET':
    #     try:
    #         return send_file(s,mimetype='image/jpg')

    #     except FileNotFoundError:
    #         abort(404)
    final_slug = 'Initial'
    if (request.method == 'POST'):
        ##########      GET SLUG        ##############
        data = json.loads(request.get_data())
        with open('data.json', 'w') as outfile:
            json.dump(data, outfile)
            skill = data['conversation']['skill']
            with open('skill.json', 'w') as outfile:
                json.dump(skill, outfile)

        if skill == 'commissions':
            nlp = data['nlp']
            intents = nlp['intents']
            with open('intents.json', 'w') as outfile:
                json.dump(intents, outfile)
            slug = intents[0]
            with open('slugs.json', 'w') as outfile:
                json.dump(slug, outfile)
            final_slug=slug['slug']
            # session['final_slug'] = final_slug
            with open('final_slug.json', 'w') as outfile:
                json.dump(final_slug, outfile)

        
        ##########      GET TITLE        ##############
        if final_slug == 'gettitle':
            position_value = data['conversation']['memory']['position']['raw']
            with open('pos_value.json', 'w') as outfile:
                json.dump(position_value, outfile)
            base_url = "https://psab-dev.callidusondemand.com/api/v2/positions?$filter=name eq "+position_value+" &select=name,title&expand=title"
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
        if skill == 'getpositions':

            position1_value = data['conversation']['memory']['position1']['raw']
            position2_value = data['conversation']['memory']['position2']['raw']

            get_positionseq1 = "https://psab-dev.callidusondemand.com/api/v2/positions?$filter=name eq "+position1_value+"&select=ruleElementOwnerSeq"
            r = requests.get(get_positionseq1 , auth=(CREDENTIALS.api_login,CREDENTIALS.api_password))
            pos1_data = json.loads(r.text)
            # pos_string1 = positionseq1.text
            # with open('positionseq1.txt', 'w+') as f:
            #     f.write(pos_string)
            positionseq1 = pos1_data['positions'][0]['ruleElementOwnerSeq']
            get_positionseq2 = "https://psab-dev.callidusondemand.com/api/v2/positions?$filter=name eq "+position2_value+"&select=ruleElementOwnerSeq"
            r = requests.get(get_positionseq2 , auth=(CREDENTIALS.api_login,CREDENTIALS.api_password))
            pos2_data = json.loads(r.text)
            # with open('positionseq2.txt', 'w+') as f:
            #     f.write(pos_string)
            positionseq2 = pos2_data['positions'][0]['ruleElementOwnerSeq']

            get_deposit = "https://psab-dev.callidusondemand.com/api/v2/deposits?$filter=((position eq "+positionseq1+" or position eq "+positionseq2+") and name eq 'DO_BaseModel_Standard_Agency_DO_Commission' and period eq 2533274790396126)&select=name, value, position&expand=position" 
            with open('get_deposit.txt', 'w') as outfile:
                json.dump(get_deposit, outfile)

            r = requests.get(get_deposit , auth=(CREDENTIALS.api_login,CREDENTIALS.api_password))

            data = json.loads(r.text)
            with open('position_data.json', 'w') as outfile:
                json.dump(data, outfile)
            df = pd.json_normalize(data['deposits']) 
            print(df)
            bx = df.plot(kind='pie', y = 'value.value', labels=df['position.displayName'], autopct='%1.2f%%')
            print(bx)
            plt.savefig('DepositCompare.png')
            return jsonify(
            status=200,
            replies=[{
            'type': 'text',
            'content': 'Chart Prepared'
            }]
            )          
    
    if (request.method == 'GET' and request.path == '/getImage'):      
        try:
            return send_file(p,mimetype='image/png')

        except FileNotFoundError:
            abort(404)
    else: 
        return send_file(s,mimetype='image/jpg')

app.run(port=port)
